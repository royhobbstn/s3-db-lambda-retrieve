"use strict";

const Parser = require('expr-eval').Parser;
const present = require('present');
const rp = require('request-promise');
const { themes } = require('./themes');

// path sent in param does not need to include attribute - can derive from expression
// don't need to differentiate between est and moe anymore


const appRouter = function(app) {

  app.get("/test", function(req, res) {
    return res.send('test');
  });

  // https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev
  // /new-retrieve?sumlev=140&cluster=0&expression=%5B%22B01001001%22%5D&dataset=acs1216

  app.get("/retrieve", function(req, res) {
    const start_time = present();

    // first run through, parses whole thing... all clusters, sends back only what was asked for and saves the rest to redis

    const theme = req.query.theme || 'mhi';
    const expression = themes[theme].numerator;
    const dataset = req.query.dataset || 'acs1216';
    const sumlev = req.query.sumlev || '140';
    const clusters = req.query.clusters ? JSON.parse(decodeURIComponent(req.query.clusters)) : ["3_5"];

    console.log({});
    console.log({ sumlev, clusters, expression, dataset });

    console.log({ time: 0, msg: 'start' });

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

    console.log(fields_key);

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

      const geoid_lookup = data.shift();

      // TODO geoid_lookup would be better as { CLUSTER: [ARRAY OF GEOIDS] }.
      // in the meantime, we'll fake it by creating it here

      console.log({ time: getTime(start_time), msg: 'needless compute start' });
      const clusters_obj = {};

      Object.keys(geoid_lookup).forEach(geoid => {
        const cluster = geoid_lookup[geoid];
        if (!clusters_obj[cluster]) {
          clusters_obj[cluster] = [];
        }
        clusters_obj[cluster].push(geoid);
      });
      console.log({ time: getTime(start_time), msg: 'needless compute end' });


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

      // send to redis

      // send back only the selected clusters
      const array_of_objects = clusters.map(cluster => {
        return data_by_cluster[cluster];
      });

      const results = Object.assign({}, ...array_of_objects);

      console.log({ time: getTime(start_time), msg: 'parsed / sending data' });

      return res.json(results);
      //
    }).catch(err => {
      console.log(err);
      return res.status(500).json({ err });
    });

  });

  // https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev
  // /retrieve?sumlev=050&cluster=0&expression=%5B%22B19013001%22%5D&dataset=acs1115 // TODO update

  app.get("/old-retrieve", function(req, res) {
    const start_time = present();

    const expression = req.query.expression ? JSON.parse(decodeURIComponent(req.query.expression)) : ["(", "(", "B01001007", "+", "B01001008", "+", "B01001009", "+", "B01001010", "+", "B01001031", "+", "B01001032", "+", "B01001033", "+", "B01001034", ")", "/", "B01001001", ")"];
    const dataset = req.query.dataset || 'acs1115';
    const sumlev = req.query.sumlev || '050';
    const clusters = req.query.clusters ? JSON.parse(decodeURIComponent(req.query.clusters)) : ["0"];

    // const expression = req.query.expression ? JSON.parse(decodeURIComponent(req.query.expression)) : ["B19013001"];
    // const dataset = req.query.dataset || 'acs1115';
    // const sumlev = req.query.sumlev || '150';
    // const clusters = req.query.clusters ? JSON.parse(decodeURIComponent(req.query.clusters)) : ["0", "1", "2", "3"];

    console.log({});
    console.log({ sumlev, clusters, expression, dataset });

    console.log({ time: 0, msg: 'start' });

    const fields = Array.from(new Set(getFieldsFromExpression(expression)));

    const parser = new Parser();
    const expr = parser.parse(expression.join(""));

    console.log({ time: getTime(start_time), msg: 'fetching s3 data' });

    const url = getUrlFromDataset(dataset);

    console.log({ time: getTime(start_time), msg: 'fetching url: ' + url });

    const datas = [];
    const fields_key = [];

    clusters.forEach(cluster => {
      fields.forEach(field => {
        fields_key.push(field);
        datas.push(rp({
          method: 'get',
          uri: `https://${url}/${field}/${sumlev}/${cluster}.json`,
          headers: {
            'Accept-Encoding': 'gzip',
          },
          gzip: true,
          json: true,
          fullResponse: false
        }));
      });
    });


    Promise.all(datas).then(data => {
      console.log({ time: getTime(start_time), msg: 'received all data' });

      // shortcut when just one field
      if (clusters.length === 1 && fields.length === 1) {
        console.log({ time: getTime(start_time), msg: 'shortcut: sending data' });
        return res.json(data[0]);
      }

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

      console.log({ time: getTime(start_time), msg: 'parsed / sending data' });

      return res.json(evaluated);
      //
    }).catch(err => {
      return res.status(500).json({ err });
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
