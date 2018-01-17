"use strict";

const Parser = require('expr-eval').Parser;
const rp = require('request-promise');
const Papa = require('papaparse');

const appRouter = function(app) {

    app.get("/test", function(req, res) {
        return res.send('test');
    });

    // curl -d '{"path":"059/050/08","geoids":["08031","08005"],"expression":["B19013001"],"moe_expression":["B19013001_moe"],"dataset":"acs1115"}' -H "Content-Type: application/json" -X POST https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev/get-parsed-expression

    // curl -d '{"path":"059/050/55","geoids":["55087","55009"],"expression":["B19013001"],"moe_expression":["B19013001_moe"],"dataset":"acs1216"}' -H "Content-Type: application/json" -X POST http://localhost:8080/get-parsed-expression

    app.post("/get-parsed-expression", function(req, res) {

        // path, geoids, fields, expression
        const path = req.body.path;
        // const geoids = req.body.geoids; // TODO geoids not necessary here?
        const expression = req.body.expression;
        const moe_expression = req.body.moe_expression;
        const dataset = req.body.dataset;

        const est_fields = Array.from(new Set(getFieldsFromExpression(expression)));
        const moe_fields = Array.from(new Set(getFieldsFromExpression(moe_expression)));

        const fields = [...est_fields, ...moe_fields];

        // const sumlev = path.split('/')[1]; // TODO sumlev not necessary here?
        const parser = new Parser();
        const expr = parser.parse(expression.join(""));
        const moe_expr = parser.parse(moe_expression.join(""));

        Promise.all([getS3Data(`e${path}.csv`, dataset), getS3Data(`m${path}.csv`, dataset)])
            .then(response => {

                const estimate = {};
                const moe = {};

                // TODO - parsing happening concurrently... not optimal

                // convert each csv to JSON with a key
                Papa.parse(response[0], {
                    header: true,
                    skipEmptyLines: true,
                    step: function(results, parser) {
                        estimate[results.data[0]['GEOID']] = results.data[0];
                    },
                    complete: function() {
                        console.log('finished est');
                    }
                });

                Papa.parse(response[1], {
                    header: true,
                    skipEmptyLines: true,
                    step: function(results, parser) {
                        moe[results.data[0]['GEOID']] = results.data[0];
                    },
                    complete: function() {
                        console.log('finished moe');
                    }
                });

                const data = {};

                // recursively combine each geoid key of estimates and moe's
                Object.keys(estimate).forEach(key => {
                    data[key] = Object.assign({}, estimate[key], moe[key]);
                });

                const evaluated = {};

                Object.keys(data).forEach((key, i) => {

                    const obj = {};
                    fields.forEach(field => {
                        obj[field] = parseFloat(data[key][field]);
                    });
                    evaluated[key] = expr.evaluate(obj);
                    evaluated[`${key}_moe`] = moe_expr.evaluate(obj);

                });

                res.json(evaluated);
            })
            .catch(err => {
                res.status(500).send(err);
            });

    });

};

module.exports = appRouter;


function getS3Data(Key, dataset) {
    const url = getUrlFromDataset(dataset);
    return rp(`https://${url}/${Key}`);
}

function getFieldsFromExpression(expression) {
    return expression.filter(d => {
        return d.length > 1;
    });
}

function getUrlFromDataset(dataset) {
    switch (dataset) {
        case 'acs1014':
            return 's3-us-west-2.amazonaws.com/s3db-v2-acs1014';
        case 'acs1115':
            return 's3-us-west-2.amazonaws.com/s3db-v2-acs1115';
        case 'acs1216':
            return 's3-us-west-2.amazonaws.com/s3db-v2-acs1216';
        default:
            console.error('unknown dataset');
            return 'maputopia.com';
    }
}
