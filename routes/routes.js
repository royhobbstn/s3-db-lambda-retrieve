"use strict";

const Parser = require('expr-eval').Parser;
const present = require('present');
const rp = require('request-promise');

// path sent in param does not need to include attribute - can derive from expression
// don't need to differentiate between est and moe anymore


const appRouter = function(app) {

  app.get("/test", function(req, res) {
    return res.send('test');
  });

  // https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev
  // /retrieve?sumlev=050&cluster=0&expression=%5B%22B19013001%22%5D&dataset=acs1115 // TODO update

  app.get("/retrieve", function(req, res) {
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
      return 's3-us-west-2.amazonaws.com/s3db-acs-1014';
    case 'acs1115':
      return 's3-us-west-2.amazonaws.com/s3db-acs-1115';
    case 'acs1216':
      return 's3-us-west-2.amazonaws.com/s3db-acs-1216';
    default:
      console.error('unknown dataset');
      return 'maputopia.com';
  }
}

function getTime(start_time) {
  return (present() - start_time).toFixed(3);
}
