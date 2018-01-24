"use strict";

const Parser = require('expr-eval').Parser;
const rp = require('request-promise');
const Papa = require('papaparse');
const present = require('present');
const zlib = require('zlib');
const https = require('https');

const appRouter = function(app) {

    app.get("/test", function(req, res) {
        return res.send('test');
    });

    // curl -d '{"path":"e059/050/08","expression":["B19013001"],"dataset":"acs1115"}' -H "Content-Type: application/json" -X POST https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev/fast-retrieve

    // curl -d '{"path":"e059/050/08","expression":["B19013001"],"dataset":"acs1115"}' -H "Content-Type: application/json" -X POST http://localhost:8080/fast-retrieve


    app.post("/fast-retrieve", function(req, res) {

        const start_time = present();

        const path = req.body.path;
        const expression = req.body.expression;
        const dataset = req.body.dataset;

        console.log({});
        console.log({ path, expression, dataset });

        console.log({ time: 0, msg: 'start' });

        const fields = Array.from(new Set(getFieldsFromExpression(expression)));

        const parser = new Parser();
        const expr = parser.parse(expression.join(""));

        // choose whether to send expression or moe_expression
        const est_or_moe = path.slice(0, 1);

        console.log({ time: getTime(start_time), msg: 'fetching s3 data' });


        const url = getUrlFromDataset(dataset);


        const gunzip = zlib.createGunzip();

        let a = 0;

        https.get(`https://${url}/${path}.csv`, function(response) {

            Papa.parse(response.pipe(gunzip), {
                header: true,
                skipEmptyLines: true,
                fastMode: true,
                chunk: function(results, parser) {

                    if (!a) {
                        console.log({ time: getTime(start_time), msg: 'first result' });
                        a++;
                    }

                    const data = {};

                    results.data.forEach(d => {
                        data[d.GEOID] = d;
                    });


                    const evaluated = {};

                    Object.keys(data).forEach((key, i) => {

                        const obj = {};
                        fields.forEach(field => {
                            // TODO is this attempted to parsefloat geoid?
                            obj[field] = parseFloat(data[key][field]);
                        });



                        if (est_or_moe === 'e') {
                            evaluated[key] = expr.evaluate(obj);
                        }
                        else {
                            evaluated[`${key}_moe`] = expr.evaluate(obj);
                        }

                    });

                    res.write(JSON.stringify(evaluated));


                },
                complete: function(response) {
                    console.log({ time: getTime(start_time), msg: 'sent s3 data' });
                    return res.end();
                }
            });


        });


    });



    app.post("/get-parsed-expression", function(req, res) {
        const start_time = present();

        const path = req.body.path;
        const expression = req.body.expression;
        const dataset = req.body.dataset;

        console.log({});
        console.log({ path, expression, dataset });

        console.log({ time: 0, msg: 'start' });

        const fields = Array.from(new Set(getFieldsFromExpression(expression)));

        const parser = new Parser();
        const expr = parser.parse(expression.join(""));

        // choose whether to send expression or moe_expression
        const est_or_moe = path.slice(0, 1);

        console.log({ time: getTime(start_time), msg: 'fetching s3 data' });

        getS3Data(`${path}.csv`, dataset)
            .then(response_gzip => {

                console.log({ time: getTime(start_time), msg: 'retrieve s3 data' });

                zlib.gunzip(response_gzip, function(err, dezipped) {
                    if (err) {
                        console.log(err);
                    }

                    const response = dezipped.toString();

                    console.log({ time: getTime(start_time), msg: 'unzipped s3 data' });

                    const data = {};

                    // convert each csv to JSON with a key
                    Papa.parse(response, {
                        header: true,
                        skipEmptyLines: true,
                        fastMode: true,
                        step: function(results, parser) {
                            data[results.data[0]['GEOID']] = results.data[0];
                        },
                        complete: function(response) {
                            // response.data.forEach(results => {
                            //     data[results['GEOID']] = results;
                            // });

                            console.log({ time: getTime(start_time), msg: 'parsed s3 data' });
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

                    console.log({ time: getTime(start_time), msg: 'sending data' });

                    return res.json(evaluated);

                });


            })
            .catch(err => {
                return res.status(500).send(err);
            });

    });

};

module.exports = appRouter;





function getS3Data(Key, dataset) {
    const url = getUrlFromDataset(dataset);
    return rp(`https://${url}/${Key}`, { encoding: null });
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

function getTime(start_time) {
    return (present() - start_time).toFixed(3);
}
