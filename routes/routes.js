"use strict";

// const AWS = require('aws-sdk');
// const s3 = new AWS.S3();
// const Parser = require('expr-eval').Parser;

const appRouter = function (app) {
    
    app.get("/test", function (req, res) {
        return res.send('test');
    });

    // app.post("/get-parsed-expression", function (req, res) {

    //     // path, geoids, fields, expression
    //     const path = req.body.path;
    //     const geoids = req.body.geoids;
    //     const expression = req.body.expression;

    //     const fields = Array.from(new Set(getFieldsFromExpression(expression)));
    //     const sumlev = path.split('/')[1];
    //     const parser = new Parser();
    //     const expr = parser.parse(expression.join(""));

    //     getS3Data(path + '.json')
    //         .then(data => {
    //             const evaluated = {};

    //             geoids.forEach(geo_part => {
    //                 const full_geoid = `${sumlev}00US${geo_part}`;

    //                 // not all geoids will be in each file.
    //                 // if they aren't here, their value will be undefined
    //                 if (data[full_geoid] !== undefined) {
    //                     const obj = {};
    //                     fields.forEach(field => {
    //                         obj[field] = parseFloat(data[full_geoid][field]);
    //                     });
    //                     evaluated[full_geoid] = expr.evaluate(obj);
    //                 }
    //             });

    //             res.json(evaluated);
    //         })
    //         .catch(err => {
    //             res.status(500).send(err);
    //         });

    // });

};

module.exports = appRouter;


// function getS3Data(Key) {
//     const Bucket = 's3db-acs1115';

//     return new Promise((resolve, reject) => {
//         s3.getObject({ Bucket, Key }, function (err, data) {
//             if (err) {
//                 console.log(err, err.stack);
//                 return reject(err);
//             }
//             const object_data = JSON.parse(data.Body.toString('utf-8'));
//             return resolve(object_data);
//         });
//     });
// }

// function getFieldsFromExpression(expression) {
//     //
//     return expression.filter(d => {
//         return d.length > 1;
//     });
// }
