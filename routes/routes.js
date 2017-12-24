"use strict";

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const Parser = require('expr-eval').Parser;

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

        Promise.all([getS3Data('e' + path + '.json', dataset), getS3Data('m' + path + '.json', dataset)])
            .then(response => {

                const data = {};

                // recursively combine each geoid key of estimates and moe's
                Object.keys(response[1]).forEach(key => {
                    data[key] = Object.assign({}, response[0][key], response[1][key]);
                });

                const evaluated = {};

                geoids.forEach((geo_part, i) => {
                    const full_geoid = `${sumlev}00US${geo_part}`;

                    // not all geoids will be in each file.
                    // if they aren't here, their value will be undefined
                    // am i really sending ALL geoids to each lambda??
                    if (data[full_geoid] !== undefined) {
                        const obj = {};
                        fields.forEach(field => {
                            obj[field] = parseFloat(data[full_geoid][field]);
                        });
                        evaluated[full_geoid] = expr.evaluate(obj);
                        evaluated[`${full_geoid}_moe`] = moe_expr.evaluate(obj);
                        evaluated[`${full_geoid}_label`] = data[full_geoid]['NAME'];
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

// TODO.  should grab from a public URL instead, so cloudfront can be involved
function getS3Data(Key, dataset) {
    const Bucket = `s3db-${dataset}`;

    return new Promise((resolve, reject) => {
        s3.getObject({ Bucket, Key }, function(err, data) {
            if (err) {
                console.log(err, err.stack);
                return reject(err);
            }
            const object_data = JSON.parse(data.Body.toString('utf-8'));
            return resolve(object_data);
        });
    });
}

function getFieldsFromExpression(expression) {
    //
    return expression.filter(d => {
        return d.length > 1;
    });
}
