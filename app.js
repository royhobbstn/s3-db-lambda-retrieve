const serverless = require('serverless-http');
const compression = require('compression');
const express = require("express");
const app = express();

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

// app.use(compression());


require("./routes/routes.js")(app);

const server = app.listen(8080, function() {
  console.log("Listening on port %s...", server.address().port);
});

module.exports.handler = serverless(app);
