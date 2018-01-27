const serverless = require('serverless-http');
const express = require("express");
const app = express();
const bodyParser = require('body-parser');

app.use(function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

app.use(function(req, res, next) {
    //
    if (!req.is('application/octet-stream')) {
        return next();
    }

    var data = []; // List of Buffer objects
    req.on('data', function(chunk) {
        data.push(chunk); // Append Buffer object
    });

    req.on('end', function() {
        if (data.length <= 0) {
            return next();
        }
        data = Buffer.concat(data); // Make one large Buffer of it
        console.log('Received buffer', data);
        req.raw = data;
        next();
    });
});


app.use(bodyParser.json());



require("./routes/routes.js")(app);

const server = app.listen(8080, function() {
    console.log("Listening on port %s...", server.address().port);
});

module.exports.handler = serverless(app);
