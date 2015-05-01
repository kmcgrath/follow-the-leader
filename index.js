var _ = require('lodash'),
EventEmitter = require("events").EventEmitter,
path = require('path'),
request = require('request'),
restify = require('restify'),
util = require('util'),
zk = require('node-zookeeper-client');

var A = function(options) {
  EventEmitter.call(this);
  this.zk_exhibitor_url = process.env.ZK_EXHIBITOR_URL;
  this.rootPath = '/testingPath';
  this.status = 'init';
};
util.inherits(A,EventEmitter);


A.prototype.getZkConnectionList = function(cb) {
  request(this.zk_exhibitor_url, function (err, response, body) {
    var jBody = JSON.parse(body);
    cb(err,jBody.servers.join(','));
  });
};


A.prototype.getClient = function(cb) {
  var self = this;
  if (this.client) {
    cb(null,this.client);
  }
  else {
    this.getZkConnectionList(function(err,list) {
      self.client = zk.createClient(list);
      cb(err,self.client);
    });
  }
};

A.prototype.start = function(cb) {
  var self = this;

  this.getClient(function(err,client) {
    client.connect();
    client.once('connected', function () {
      console.log('Connected to ZooKeeper.');
      client.mkdirp(self.rootPath, function (error, path) {
        console.log('Node: %s is created.', path);
        self.checkBootstrapped(client,function(err,r) {
          cb();
        });
      });
    })
  });

};

A.prototype.checkBootstrapped = function(client,cb) {
  var self = this;
  console.log("checking bootstrap");
  client.exists(path.join(this.rootPath,'bootstrapped'), function (err,stat) {
    if (stat) {
      console.log("already bootstrapped",stat);
      cb(null);
    } else {
      self.checkBootstrapping(client,function(err) {
        cb(err);
      });
    }
  });
};

A.prototype.checkBootstrapping = function(client,cb) {
  var self = this;
  client.exists(path.join(this.rootPath,'bootstrapping'), function (err,stat) {
    if (stat) {
      self.status = 'waiting';
      self.checkBootstrapped(client,cb);
    } else {
      self.createBootstrapBarrier(client,function(err,path) {
        cb(err);
      });
    }
  });
};


A.prototype.createBootstrapBarrier = function(client,cb) {
  var self = this;
  client.create(path.join(this.rootPath,'bootstrapping'), '', zk.CreateMode.EPHEMERAL, function (err) {
    if (err) {
      console.log('Failed to create node: %s due to: %s.', path, err);
      cb(err);
    } else {
      console.log('Node: %s is successfully created.', path);
      self.status = 'blocking';
      cb(null,path);
    }
  });
}

A.prototype.unblock = function(client,data,cb) {
  var self = this;
  client.create(path.join(this.rootPath,'bootstrapped'), data, zk.CreateMode.EPHEMERAL, function (err) {
    if (err) {
      console.log('Failed to create node: %s due to: %s.', path, err);
      cb(err);
    } else {
      console.log('Node: %s is successfully created.', path);
      self.status = 'bootstrapped';
      cb(null,path);
    }
  });
};

A.prototype.getStatus = function() {
  return {
    status: this.status
  };
};



var a = new A();
a.start(function() {
  console.log("started")
});

var server = restify.createServer({
  name: 'myapp',
  version: '1.0.0'
});
server.use(restify.acceptParser(server.acceptable));
server.use(restify.queryParser());
server.use(restify.bodyParser());

server.get('/status', function(req,res,next) {
  res.send(a.getStatus());
  next();
});

server.post('/unblock', function(req,res,next) {
  if (a.getStatus().status === 'blocking') {
    a.getClient(function(err,client) {
      a.unblock(client,res.body,function(err,r) {
        if (err) {
          res.send(400,"Failed to unblock: " + err);
        }
        else {
          res.send(200,"unblocked!");
        }
        next();
      });
    });
  }
  else {
    res.send(400,"Error: Must be in blocking state to unblock");
    next();
  }
});


server.listen(8080, function () {
  console.log('%s listening at %s', server.name, server.url);
});
