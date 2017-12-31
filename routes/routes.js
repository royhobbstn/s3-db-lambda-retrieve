"use strict";

const Parser = require('expr-eval').Parser;
const rp = require('request-promise');

const appRouter = function(app) {

    app.get("/test", function(req, res) {
        return res.send('test');
    });

    // curl -d '{"path":"059/050/08","geoids":["08031","08005"],"expression":["B19013001"],"moe_expression":["B19013001_moe"],"dataset":"acs1115"}' -H "Content-Type: application/json" -X POST https://d0ahqlmxvi.execute-api.us-west-2.amazonaws.com/dev/get-parsed-expression
    app.post("/get-parsed-expression", function(req, res) {

        // path, geoids, fields, expression
        const path = req.body.path;
        const geoids = req.body.geoids;
        const expression = req.body.expression;
        const moe_expression = req.body.moe_expression;
        const dataset = req.body.dataset;

        const est_fields = Array.from(new Set(getFieldsFromExpression(expression)));
        const moe_fields = Array.from(new Set(getFieldsFromExpression(moe_expression)));

        const fields = [...est_fields, ...moe_fields];

        const sumlev = path.split('/')[1];
        const parser = new Parser();
        const expr = parser.parse(expression.join(""));
        const moe_expr = parser.parse(moe_expression.join(""));

        Promise.all([getS3Data(`e${path}.json`, dataset), getS3Data(`m${path}.json`, dataset)])
            .then(response => {

                const estimate = JSON.parse(response[0]);
                const moe = JSON.parse(response[1]);

                const data = {};

                // recursively combine each geoid key of estimates and moe's
                Object.keys(estimate).forEach(key => {
                    data[key] = Object.assign({}, estimate[key], moe[key]);
                });

                const evaluated = {};

                geoids.forEach((geo_part, i) => {
                    const full_geoid = `${sumlev}00US${geo_part}`;

                    if (data[full_geoid] !== undefined) {
                        const obj = {};
                        fields.forEach(field => {
                            obj[field] = parseFloat(data[full_geoid][field]);
                        });
                        evaluated[full_geoid] = expr.evaluate(obj);
                        evaluated[`${full_geoid}_moe`] = moe_expr.evaluate(obj);
                        evaluated[`${full_geoid}_label`] = data[full_geoid]['NAME'];
                    }
                    else {
                        console.log(`undefined value: ${full_geoid}`);
                    }
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
    const cdn_url = getUrlFromDataset(dataset);
    return rp(`https://${cdn_url}/${Key}`);
}

function getFieldsFromExpression(expression) {
    return expression.filter(d => {
        return d.length > 1;
    });
}

function getUrlFromDataset(dataset) {
    switch (dataset) {
        case 'acs1014':
            return 'd2y228x0z69ksn.cloudfront.net';
        case 'acs1115':
            return 'd1r5yvgf798u6b.cloudfront.net';
        case 'acs1216':
            return 'd23tgl2ix1iyqu.cloudfront.net';
        default:
            console.error('unknown dataset');
            return 'maputopia.com';
    }
}
