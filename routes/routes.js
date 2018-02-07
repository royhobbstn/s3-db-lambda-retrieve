"use strict";

const Parser = require('expr-eval').Parser;
const present = require('present');
const rp = require('request-promise');


const appRouter = function(app) {

    app.get("/test", function(req, res) {
        return res.send('test');
    });

    // https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev
    // /retrieve?path=e002/140/89&expression=%5B%22B01001001%22%5D&dataset=acs1216

    app.get("/retrieve", function(req, res) {

        const start_time = present();

        const path = decodeURIComponent(req.query.path);
        const expression = JSON.parse(decodeURIComponent(req.query.expression));
        const dataset = req.query.dataset;

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

        rp({
            method: 'get',
            uri: `https://${url}/${path}.json`,
            headers: {
                'Accept-Encoding': 'gzip',
            },
            gzip: true,
            json: true,
            fullResponse: false
        }).then(data => {
            //
            console.log({ time: getTime(start_time), msg: 'received and parsed s3 data' });

            const evaluated = {};

            Object.keys(data).forEach((key, i) => {

                const obj = {};
                fields.forEach(field => {
                    obj[field] = data[key][field];
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
