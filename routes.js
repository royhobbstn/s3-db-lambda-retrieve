"use strict";

const Parser = require('expr-eval').Parser;
const present = require('present');
const rp = require('request-promise');
const { client } = require('./redis.js');
const { themes } = require('./themes');

// path sent in param does not need to include attribute - can derive from expression
// don't need to differentiate between est and moe anymore


const appRouter = function(app) {

  app.get("/test", function(req, res) {

    client.set("key", "val");

    client.get("key", function(err, reply) {
      if (err) {
        return res.json({ err: 'error' });
      }

      // reply is null when the key is missing
      console.log(reply);
      return res.send(reply);
    });

  });

  app.get("/flush", function(req, res) {

    client.flushdb(function(err, reply) {
      if (err) {
        console.log(err);
        return res.json({ err });
      }
      return res.json({ response: 'cleared' });
    });

  });

  // https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev
  // /new-retrieve?sumlev=140&cluster=0&expression=%5B%22B01001001%22%5D&dataset=acs1216

  app.get("/retrieve", function(req, res) {
    const start_time = present();

    // first run through, parses whole thing... all clusters, sends back only what was asked for and saves the rest to redis

    const theme = req.query.theme || 'mhi';
    const expression = req.query.expression ? JSON.parse(decodeURIComponent(req.query.expression)) : ["B19013001_moe"]; // themes[theme].numerator;
    const dataset = req.query.dataset || 'acs1216';
    const sumlev = req.query.sumlev || '140';
    const e_or_m = req.query.moe ? 'm' : 'e';
    const clusters = req.query.clusters ? JSON.parse(decodeURIComponent(req.query.clusters)) : ["3_5"];

    console.log({});
    console.log({ sumlev, clusters, expression, dataset, e_or_m });

    console.log({ time: 0, msg: 'start' });

    // redis key will be dataset:theme:sumlev:e_or_m:cluster
    const redis_keys = clusters.map(cluster => {
      return `${dataset}:${theme}:${sumlev}:${e_or_m}:${cluster}`;
    });

    console.log({ redis_keys });

    // query redis for needed keys
    client.mget(redis_keys, function(err, reply) {
      if (err) {
        console.log(err);
        return res.json({ err: 'error' });
      }

      // reply is null when the key is missing
      // make sure there are replies for every cluster
      const has_all_data = reply.every(cluster_data => {
        return cluster_data;
      });

      console.log('all data available in redis: ', has_all_data);

      if (has_all_data) {

        const parsed_replay = reply.map(cluster_data => {
          return JSON.parse(cluster_data);
        });

        const results = Object.assign({}, ...parsed_replay);
        console.log({ time: getTime(start_time), msg: 'sending redis data' });
        return res.json(results);
      }

      // if we don't have all cluster data, will fall through to object below

      const fields = Array.from(new Set(getFieldsFromExpression(expression)));

      const parser = new Parser();
      const expr = parser.parse(expression.join(""));

      console.log({ time: getTime(start_time), msg: 'fetching s3 data' });

      const url = getUrlFromDataset(dataset);

      console.log({ time: getTime(start_time), msg: 'fetching url: ' + url });

      const datas = [];
      const fields_key = [];


      fields.forEach(field => {
        fields_key.push(field);
        datas.push(rp({
          method: 'get',
          uri: `https://${url}/${field}/${sumlev}.json`,
          headers: {
            'Accept-Encoding': 'gzip',
          },
          gzip: true,
          json: true,
          fullResponse: false
        }));
      });

      console.log({ fields_key });

      const geo_year = getGeoYearFromDataset(dataset);
      const geog = getGeographyLevelFromSumlev(sumlev);

      const geoid_lookup = rp({
        method: 'get',
        uri: `https://s3-us-west-2.amazonaws.com/geo-metadata/clusters_${geo_year}_${geog}.json`,
        headers: {
          'Accept-Encoding': 'gzip',
        },
        gzip: true,
        json: true,
        fullResponse: false
      });

      Promise.all([geoid_lookup, ...datas]).then(data => {
        console.log({ time: getTime(start_time), msg: 'received all data' });

        const clusters_obj = data.shift();

        // we don't really need to get only a few clusters at a time.
        // we can calculate all data, cluster it, save it to redis
        // then pick just the initial selected clusters

        // combine clusters
        const combined_data = [];

        // create a data structure where combined_data indexes match fields indexes
        fields_key.forEach((field_key, field_key_index) => {
          fields.forEach((field, i) => {
            if (field_key === field) {
              if (combined_data[i]) {
                combined_data[i] = Object.assign({}, combined_data[i], data[field_key_index]);
              }
              else {
                combined_data[i] = data[field_key_index];
              }
            }
          });
        });


        const evaluated = {};

        // combined_data[0] index is arbitrary.  goal is just to loop through all geoids
        // create a mini object where each object key is a field name.
        // then solve the expression, and record the result in a master 'evaluated' object
        Object.keys(combined_data[0]).forEach(geoid => {
          const obj = {};
          fields.forEach((field, i) => {
            obj[field] = combined_data[i][geoid];
          });
          evaluated[geoid] = expr.evaluate(obj);

        });


        const data_by_cluster = {};

        // divide into clusters
        Object.keys(clusters_obj).forEach(cluster => {
          //
          if (!data_by_cluster[cluster]) {
            data_by_cluster[cluster] = {};
          }

          clusters_obj[cluster].forEach(geoid => {
            data_by_cluster[cluster][geoid] = evaluated[geoid];
          });

        });

        const redis_set_promises = [];
        // send to redis
        Object.keys(data_by_cluster).forEach(cluster => {
          const key = `${dataset}:${theme}:${sumlev}:${e_or_m}:${cluster}`;
          const pr = client.set(key, JSON.stringify(data_by_cluster[cluster]));
          redis_set_promises.push(pr);
        });

        // wait until we're sure Redis has saved everything
        Promise.all(redis_set_promises).then(() => {
          // send back only the selected clusters
          const array_of_objects = clusters.map(cluster => {
            return data_by_cluster[cluster];
          });

          const results = Object.assign({}, ...array_of_objects);

          console.log({ time: getTime(start_time), msg: 'parsed / sending data' });

          return res.json(results);
        });


        //
      }).catch(err => {
        console.log(err);
        return res.status(500).json({ err });
      });

    });



  });

};

module.exports = appRouter;



function getFieldsFromExpression(expression) {
  return expression.filter(d => {
    return d.length > 1;
  });
}

function getUrlFromDataset(dataset) {

  switch (dataset) {
    case 'acs1014':
      return `s3-us-west-2.amazonaws.com/s3db-acs-1014`;
    case 'acs1115':
      return `s3-us-west-2.amazonaws.com/s3db-acs-1115`;
    case 'acs1216':
      return `s3-us-west-2.amazonaws.com/s3db-acs-1216`;
    default:
      console.error('unknown dataset');
      return 'maputopia.com';
  }
}

function getTime(start_time) {
  return (present() - start_time).toFixed(3);
}

function getGeoYearFromDataset(dataset) {
  switch (dataset) {
    case 'acs1014':
      return `2014`;
    case 'acs1115':
      return `2015`;
    case 'acs1216':
      return `2016`;
    default:
      console.error('unknown dataset');
      return '0000';
  }
}

function getGeographyLevelFromSumlev(sumlev) {
  switch (sumlev) {
    case '040':
      return `state`;
    case '050':
      return `county`;
    case '140':
      return `tract`;
    case '150':
      return `bg`;
    case '160':
      return `place`;
    default:
      console.error('unknown sumlev');
      return 'unknown';
  }
}
