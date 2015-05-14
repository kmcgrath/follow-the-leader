var _ = require('lodash'),
EventEmitter = require("events").EventEmitter,
path = require('path'),
request = require('request'),
restify = require('restify'),
util = require('util'),
zk = require('./lib/discovery/zk');


var zk = new zk();

zk.start();

var server = restify.createServer({
  name: 'myapp',
  version: '1.0.0'
});
server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());

server.get('/status', function(req,res,next) {
  res.send(zk.getStatus());
  next();
});

server.post('/bootstrap', function(req,res,next) {
  if (zk.getStatus().status === 'bootstrap') {
      zk.bootstrap(res.body,function(err,r) {
        if (err) {
          res.send(400,"Failed to bootstrap: " + err);
        }
        else {
          res.send(200,"bootstrapped!");
        }
        next();
      });
  }
  else {
    res.send(400,"Error: Must be in bootstrap state to bootstrap");
    next();
  }
});


server.listen(process.env.PORT || 8080, function () {
  console.log('%s listening at %s', server.name, server.url);
});
