var vogels = require('vogels'),
    Joi    = require('joi');


var Job = vogels.define('Job', {
  hashKey : '_id',
  timestamps : true,
  schema : {
    _id : vogels.types.uuid(),
    name : Joi.string(),
    type : Joi.string(),
    priority : Joi.number().default(0),
    data : Joi.any().allow(null),
    repeatInterval : Joi.any(),
    repeatTimezone : Joi.string().allow(null),
    repeatAt : Joi.any(),
    failReason : Joi.string().allow(null),
    failCount : Joi.number().default(0),
    failedAt : Joi.any(),
    nextRunAt : Joi.any(),
    lastRunAt : Joi.any(),
    lastFinishedAt : Joi.any(),
    lastModifiedBy: Joi.string().allow(null),
    lockedAt : Joi.any().default(new Date(1)),
    disabled : Joi.boolean().default(false),
  },
  indexes : [
    { hashKey : 'name', type: 'global', name : 'NameIndex' },
    { hashKey : 'name', rangeKey : 'type', type: 'global', name : 'NameTypeIndex' },
    { hashKey : 'name', rangeKey : 'priority', type: 'global', name : 'NamePriorityIndex' },
  ]
});

var DynamoDBAdapter = module.exports = function(agenda, config, cb) {
  this._agenda = agenda;
  this._vogels = vogels;
  this._collection = Job;
  this._tableName = config.db ? config.db.collection : undefined;

  if (config.connection) {
    this.connection(config.connection, config.db ? config.db.collection : undefined, cb);
  } else if (config.db && config.db.credentials) {
    this.database(config.db.credentials, config.db.collection, cb);
  }
};

DynamoDBAdapter.prototype.hasConnection = function() {
  return true;
};

// Configuration Methods

DynamoDBAdapter.prototype.connection = function( dynamodb, collection, cb ){
  this._collection.config({dynamodb: dynamodb });
  this.db_init(collection, cb);
  return this;
};

DynamoDBAdapter.prototype.database = function(credentials, collection, cb) {
  if (typeof credentials !== "object") {
    this._vogels.AWS.config.loadFromPath(credentials);
  } else {
    this._vogels.AWS.config.update(credentials);
  }

  this.db_init( collection, cb );
  return this;
};

DynamoDBAdapter.prototype.db_init = function( collection, cb ){
  Job.config({tableName: this._tableName || collection || 'agendaJobs'});

  this._vogels.createTables(function(err) {
    if (cb) {
      cb(err, Job);
    }
  });
};

function cleanData(obj) {
  var keys = Object.keys(obj);
  keys.forEach(function(key) {
    if (obj[key] === undefined) {
      obj[key] = null;
    }
  });

  return obj;
}

function getQueryFromObject(self, obj) {
  var query;

  console.log('getQueryFromObject', obj);

  if (obj['_id']) {
    query = self._collection.query(obj['_id']);
    delete obj['_id'];
  } else if (obj['name'] && obj['priority']) {
    query = self._collection.query(obj['name'])
      .usingIndex('NamePriorityIndex')
      .where('priority').eqauls(obj['priority']);
    delete obj['name'];
    delete obj['priority'];
  } else if (obj['name'] && obj['type']) {
    query = self._collection.query(obj['name'])
      .usingIndex('NameTypeIndex')
      .where('type').eqauls(obj['type']);
    delete obj['name'];
    delete obj['type'];
  } else if (obj['name']) {
    query = self._collection.query(obj['name'])
      .usingIndex('NameIndex');
    delete obj['name'];
  } else {
    return self._collection.scan();
  }

  var keys = Object.keys(obj);
  keys.forEach(function(key) {
    query = query.filter(key).equals(obj[key]);
  });

  return query;
}

DynamoDBAdapter.prototype.jobs = function(query, cb){
  if (typeof query === 'object') {
    query = getQueryFromObject(this, query);
  }

  query.exec( function( err, result ){
    var items = !result.Items ? [result.attrs] : result.Items.map(function(obj) {
      return obj.attrs;
    });

    cb( err, items );
  });
};

DynamoDBAdapter.prototype.purge = function(definedNames, cb) {
  var self = this;
  var filter = "";
  var values = {};

  definedNames.forEach(function(value, i) {
    if (i > 0) {
      filter += ' AND ';
    }

    filter += `#name <> :name${i}`;
    values[`:name${i}`] = value;
  });

  this._collection.scan().filterExpression(filter).expressionAttributeValues(values)
    .expressionAttributeNames({'#name': 'name'}).projectionExpression('#name')
    .attributes(['_id']).exec(function(error, response) {
    var promises = response.Items.map(function(value) {
      return new Promise(function(resolve) {
        self._collection.destroy({_id: value.attrs._id}, function (err, acc) {
          resolve(err, acc);
        });
      });
    });

    Promise.all(promises).then(function(results) {
      var errors = [], objects = [];

      results.forEach(function(result) {
        if (result[0]) {
          errors.push(result[0]);
        }
        objects.push(result[1]);
      });

      cb(errors, objects);
    });
  });
};

DynamoDBAdapter.prototype.cancel = function(query, cb) {
  if (typeof query === 'object') {
    query = getQueryFromObject(this, query);
  }

  var self = this;

  query.exec(function(err, response) {
    var items = response.Items ? response.Items : [response];

    var promises = items.map(function(value) {
      return new Promise(function(resolve) {
        self._collection.destroy({_id: value.attrs._id}, function (err, acc) {
          resolve(err, acc);
        });
      });
    });

    Promise.all(promises).then(function(results) {
      var errors = [], objects = [];

      results.forEach(function(result) {
        if (result[0]) {
          errors.push(result[0]);
        }
        objects.push(result[1]);
      });

      cb(errors, objects);
    });
  });
};

DynamoDBAdapter.prototype.saveByID = function(id, props, cb) {
  props._id = id;

  props = cleanData(props);

  this._collection.update(props, function (err, acc) {
    if (!err) {
      cb(err, {
        id: acc.attrs._id,
        nextRunAt: acc.attrs.nextRunAt
      });
    } else {
      cb(err, null);
    }
  });
};

DynamoDBAdapter.prototype.saveSingle = function(name, type, props, insertOnly, cb) {
  var self = this;

  this._collection.query(name).usingIndex('NameTypeIndex')
    .where('type').equals(type).exec(function (err, response) {
      if (err || response.Items.length < 1) {
        var keys = Object.keys(insertOnly);
        keys.forEach(function(key) {
          props[key] = insertOnly[key];
        });

        self.insert(props, cb);
      } else {
        self.saveByID(response.Items[0].attrs._id, props, cb);
      }
    });
};

DynamoDBAdapter.prototype.saveUnique = function(name, query, props, insertOnly, cb) {
  var self = this;
  var q = this._collection.query(name).usingIndex('NameIndex');

  var keys = Object.keys(query);
  keys.forEach(function(key) {
    q.filterExpression('#title < :t')
      .expressionAttributeValues({ ':t' : 'Expanding' })
      .expressionAttributeNames({ '#title' : 'title'});
    q.filter(key).equals(query[key]);
  });

  console.log('saveUnique', name, query);

  q.exec(function (err, response) {
    if (err || response.Items.length < 1) {
      self.insert(props, cb);
    } else {
      self.saveByID(response.Items[0].attrs._id, insertOnly, cb);
    }
  });
};

DynamoDBAdapter.prototype.insert = function(insert, cb) {
  props = cleanData(insert);

  this._collection.create(insert, function (err, acc) {
    console.log('insert', err, acc)
    if (err) {
      cb(err, acc);
      return;
    }

    cb(err, {
      id: acc.attrs._id,
      nextRunAt: acc.attrs.nextRunAt
    });
  });
};

DynamoDBAdapter.prototype._unlockJobs = function(jobIds, done) {
  var notLocked = new Date(1);
  var self = this;

  var promises = jobIds.map(function(value) {
    return new Promise(function(resolve) {
      self._collection.update({_id: value, lockedAt: notLocked.toISOString()}, function (err, acc) {
        resolve(err, acc);
      });
    });
  });

  Promise.all(promises).then(function(results) {
    var errors = [], objects = [];

    results.forEach(function(result) {
      if (result[0]) {
        errors.push(result[0]);
      }
      objects.push(result[1]);
    });

    done(errors, objects);
  });
};

DynamoDBAdapter.prototype._findAndLockNextJob = function(jobName, nextScanAt, lockDeadline, cb) {
  var now = new Date();
  var self = this;

  this._collection.query(jobName).usingIndex('NamePriorityIndex').ascending()
    .filter('disabled').ne(true)
    .filter('lockedAt').lte(lockDeadline.toISOString())
    .filter('nextRunAt').lte(nextScanAt.toISOString())
    .exec(function(error, response) {
      if (!error && response.Items.length > 0) {
        self._collection.update({_id: response.Items[0].attrs._id, lockedAt: now.toISOString() }, function(err, result) {
          cb(err, result.attrs);
        });
      } else {
        cb(error, null);
      }
    });
};

DynamoDBAdapter.prototype.lockOnTheFly = function(job, cb) {
  var notLocked = new Date(1);
  var now = new Date();
  var self = this;

  this._collection.query(job.attrs._id)
    .filter('lockedAt').equals(notLocked.toISOString())
    .filter('nextRunAt').equals(job.attrs.nextRunAt.toISOString())
    .filter('disabled').ne(true)
    .exec(function (error, response) {
      console.log('lockOnTheFly', error, response);
      if (!error && response.Items.length > 0) {
        self._collection.update({_id: job.attrs._id, lockedAt: now.toISOString() }, function(err, result) {
          console.log('lockOnTheFly2', err, result);
          cb(err, result.attrs);
        });
        return;
      } else {
        cb(error, null);
      }
    });
};
