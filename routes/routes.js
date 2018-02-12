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
    // /retrieve?sumlev=050&cluster=0&expression=%5B%22B19013001%22%5D&dataset=acs1115

    app.get("/retrieve", function(req, res) {

        const start_time = present();

        const sumlev = req.query.sumlev;
        const cluster = req.query.cluster;
        const expression = JSON.parse(decodeURIComponent(req.query.expression));
        const dataset = req.query.dataset;

        console.log({});
        console.log({ sumlev, cluster, expression, dataset });

        console.log({ time: 0, msg: 'start' });

        const fields = Array.from(new Set(getFieldsFromExpression(expression)));

        const parser = new Parser();
        const expr = parser.parse(expression.join(""));

        console.log({ time: getTime(start_time), msg: 'fetching s3 data' });

        const url = getUrlFromDataset(dataset);

        console.log({ time: getTime(start_time), msg: 'fetching url: ' + url });

        const datas = fields.map(field => {
            return rp({
                method: 'get',
                uri: `https://${url}/${field}/${sumlev}/${cluster}.json`,
                headers: {
                    'Accept-Encoding': 'gzip',
                },
                gzip: true,
                json: true,
                fullResponse: false
            });
        });

        Promise.all(datas).then(data => {
            // TODO shortcut when just one field

            console.log({ time: getTime(start_time), msg: 'received all data' });

            const evaluated = {};

            Object.keys(data[0]).forEach(geoid => {
                const obj = {};
                fields.forEach((field, i) => {
                    obj[field] = data[i][geoid];
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
