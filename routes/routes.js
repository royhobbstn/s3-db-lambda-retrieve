"use strict";

const Parser = require('expr-eval').Parser;
const Papa = require('papaparse');
const present = require('present');
const zlib = require('zlib');
const https = require('https');


let path = require('path');

let ProtoBuf = require('protobufjs');
let builder = ProtoBuf.loadProtoFile(path.join(__dirname, 'message.proto'));
let Pair = builder.build('Pair');
let Dictionary = builder.build('Dictionary');


const appRouter = function(app) {

    app.post('/pbf', (req, res, next) => {


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
                            // TODO is this attempting to parsefloat geoid?
                            obj[field] = parseFloat(data[key][field]);
                        });


                        if (est_or_moe === 'e') {
                            evaluated[key] = expr.evaluate(obj);
                        }
                        else {
                            evaluated[`${key}_moe`] = expr.evaluate(obj);
                        }

                    });

                    // protobuf Pair array
                    const pairs = Object.keys(evaluated).map(key => {
                        return new Pair({ key, value: String(evaluated[key]) || '' });
                    });

                    let msg = new Dictionary({ pairs });

                    // res.write(JSON.stringify(evaluated));
                    res.write(msg.encode().toBuffer());

                },
                complete: function(response) {
                    console.log({ time: getTime(start_time), msg: 'sent s3 data' });
                    return res.end();
                }
            });

        });


    });

    // app.post('/api/messages', (req, res, next) => {
    //     if (req.raw) {
    //         try {
    //             // Decode the Message
    //             var msg = Message.decode(req.raw);
    //             console.log('Received "%s" in %s', msg.text, msg.lang);
    //         }
    //         catch (err) {
    //             console.log('Processing failed:', err);
    //             next(err);
    //         }
    //     }
    //     else {
    //         console.log("Not binary data");
    //     }
    // });

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
                            // TODO is this attempting to parsefloat geoid?
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
