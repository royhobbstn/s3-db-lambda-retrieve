"use strict";

const Parser = require('expr-eval').Parser;
const rp = require('request-promise');
const Papa = require('papaparse');
const uuidv4 = require('uuid/v4');
const present = require('present');

const appRouter = function(app) {

    app.get("/test", function(req, res) {
        return res.send('test');
    });

    // curl -d '{"path":"e059/050/08","expression":["B19013001"],"dataset":"acs1115"}' -H "Content-Type: application/json" -X POST https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev/get-parsed-expression

    // curl -d '{"path":"e059/050/55","expression":["B19013001"],"dataset":"acs1216"}' -H "Content-Type: application/json" -X POST http://localhost:8080/get-parsed-expression

    // curl -d '{"path":"m059/050/55", "expression":["B19013001_moe"],"dataset":"acs1216"}' -H "Content-Type: application/json" -X POST http://localhost:8080/get-parsed-expression

    app.post("/get-parsed-expression", function(req, res) {
        const uuid = uuidv4();
        const start_time = present();

        const path = req.body.path;
        const expression = req.body.expression;
        const dataset = req.body.dataset;

        console.log('\n' + uuid, path, expression, dataset);
        console.log(uuid, 0, 'starting function');

        const fields = Array.from(new Set(getFieldsFromExpression(expression)));

        const parser = new Parser();
        const expr = parser.parse(expression.join(""));

        // choose whether to send expression or moe_expression
        const est_or_moe = path.slice(0, 1);

        console.log(uuid, present() - start_time, 'fetching s3 data');
        getS3Data(`${path}.csv`, dataset)
            .then(response => {

                console.log(uuid, present() - start_time, 'retrieve s3 data');

                const data = {};

                // convert each csv to JSON with a key
                Papa.parse(response, {
                    header: true,
                    skipEmptyLines: true,
                    step: function(results, parser) {
                        data[results.data[0]['GEOID']] = results.data[0];
                    },
                    complete: function() {
                        console.log(uuid, present() - start_time, 'parsed s3 data');
                    }
                });


                const evaluated = {};

                Object.keys(data).forEach((key, i) => {

                    const obj = {};
                    fields.forEach(field => {
                        obj[field] = parseFloat(data[key][field]);
                    });

                    if (est_or_moe === 'e') {
                        evaluated[key] = expr.evaluate(obj);
                    }
                    else {
                        evaluated[`${key}_moe`] = expr.evaluate(obj);
                    }

                });

                console.log(uuid, present() - start_time, 'sending data.  ending function');

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
