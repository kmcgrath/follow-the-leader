var _ = require('lodash'),
async = require('async'),
EventEmitter = require("events").EventEmitter,
path = require('path'),
request = require('request'),
restify = require('restify'),
util = require('util'),
zk = require('node-zookeeper-client');

var Discovery = function(options) {
  var self = this;

  EventEmitter.call(this);
  this.paths = {};
  this.leader = false;
  this.nodeZpath = null;

  this.zk_exhibitor_url = process.env.ZK_EXHIBITOR_URL;
  this.rootPath = '/testing2Path';

  this.on('start', function createClient() {
    self.getClient(function clientCreated(error,client) {
      //TODO Error
      self.emit('client:created',client);
    });
  });

  this.once('client:created', function connect(client) {
    client.connect();
    client.once('connected', function () {
      self.emit('client:connected',client);
    })
  });

  this.once('client:connected', function connect(client) {
    self.emit('state','initialized');
  });

  this.on('state', function stateChange(state) {
    console.log('state change: ' + state);
    self.state = state;
    self.emit('state:'+state);
  });

  this.on('state:initialized', function() {
    async.parallel(
      [
        function(cb) {
          self.client.mkdirp(path.join(self.rootPath,'nodes'), function(err, path) {
            self.emit('path:exists','nodes');
            cb();
          })
        }
      ],
      function pathsExist(err,results) {
        self.emit('paths:complete');
      }
    )
  });


  this.on('paths:complete', self.tryToLead);

  this.on('leader', function(isLeader,nodes) {
    if (isLeader) {
      if (self.nodeZpath) {
        // Already Joined
        self.emit('state', 'node:leader');
      }
      else {
        // Bootstrap Cluster
        self.emit('state', 'bootstrap');
      }
    }
    else {
      self.client.exists(path.join(self.rootPath,'leader'),self.tryToLead.bind(self),function(){});
      if (self.nodeZpath) {
        // Already Joined
        self.emit('state', 'node:active');
      }
      else if (nodes && nodes.length == 0) {
        // Wait for bootstrap
        self.client.getChildren(path.join(self.rootPath,'nodes'),self.tryToLead.bind(self),function(){});
        self.emit('state', 'wait');
      }
      else {
        // Join Cluster
        self.emit('state', 'join');
      }
    }
  });

};
util.inherits(Discovery,EventEmitter);

Discovery.prototype.tryToLead = function() {
  var self = this;

  self.client.getChildren(
    path.join(self.rootPath,'nodes'),
    function(err,children,stats) {
      if(err) {
        console.log(err);
      }
      else {
        if (children.length === 0 || (children.length > 0 && self.nodeZpath)) {
          self.client.create(
            path.join(self.rootPath,'leader'),
            '',
            zk.CreateMode.EPHEMERAL,
            function(err,zPath) {
              if (err) {
                self.leader = false;
                self.emit('leader',false,children);
              }
              else {
                self.leader = true;
                self.emit('leader',true,children);
              }
            }
          )
        }
        else {
          self.emit('leader',false,children);
        }
      }
    }
  )
};

Discovery.prototype.getZkConnectionList = function(cb) {
  request(this.zk_exhibitor_url, function (err, response, body) {
    var jBody = JSON.parse(body);
    cb(err,jBody.servers.join(','));
  });
};


Discovery.prototype.getClient = function(cb) {
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


Discovery.prototype.start = function(cb) {
  var self = this;
  this.emit('start');
};


Discovery.prototype.unblock = function(data,cb) {
  var self = this;
  self.client.create(path.join(this.rootPath,'nodes','node'), data, zk.CreateMode.EPHEMERAL_SEQUENTIAL, function (err,zPath) {
    if (err) {
      console.log('Failed to create node: %s due to: %s.', zPath, err);
      cb(err);
    } else {
      console.log('Node: %s is successfully created.', zPath);
      self.nodeZpath = zPath;
      self.emit('leader',true,[]);
      cb(null,zPath);
    }
  });
};

Discovery.prototype.getStatus = function() {
  return {
    status: this.state
  };
};

module.exports = Discovery;
