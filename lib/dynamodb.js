var vogels = require('vogels'),
    Joi    = require('joi');

function getCollection(tableName) {
  return vogels.define('Job', {
    hashKey : 'name',
    rangeKey: '_id',
    timestamps : true,
    schema : {
      name : Joi.string(),
      _id : vogels.types.uuid(),
      type : Joi.string(),
      priority : Joi.number().default(0),
      data : Joi.any().allow(null),
      repeatInterval : Joi.any(),
      repeatTimezone : Joi.string().allow(null),
      repeatAt : Joi.any(),
      failReason : Joi.string().allow(null),
      failCount : Joi.number().default(0),
      failedAt : Joi.any(),
      nextRunAt : Joi.any().default((new Date(1))/1),
      lastRunAt : Joi.any(),
      lastFinishedAt : Joi.any(),
      lastModifiedBy: Joi.string().allow(null),
      lockedAt : Joi.any().default((new Date(1))/1),
      disabled : Joi.boolean().default(false),
    },
    indexes : [
      { hashKey : '_id', type: 'global', name : 'IDIndex' },
      { hashKey : 'name', rangeKey : 'type', type: 'local', name : 'NameTypeIndex' },
      { hashKey : 'name', rangeKey : 'priority', type: 'local', name : 'NamePriorityIndex' },
    ],
    tableName: tableName
  });
}

var DynamoDBAdapter = module.exports = function(agenda, config, cb) {
  this._agenda = agenda;
  this._vogels = vogels;
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
  if (!this._collection) {
    this._collection = getCollection(this._tableName || collection || 'agendaJobs');
  }
  this._collection.config({dynamodb: dynamodb });
  this.db_init(cb);
  return this;
};

DynamoDBAdapter.prototype.database = function(credentials, collection, cb) {
  if (typeof credentials !== "object") {
    this._vogels.AWS.config.loadFromPath(credentials);
  } else {
    this._vogels.AWS.config.update(credentials);
  }

  if (!this._collection) {
    this._collection = getCollection(this._tableName || collection || 'agendaJobs');
  }

  this.db_init(cb );
  return this;
};

DynamoDBAdapter.prototype.db_init = function(cb ){
  var self = this;

  this._vogels.createTables(function(err) {
    self._agenda.emit('ready');
    if (cb) {
      cb(err, self._collection);
    }
  });
};

function cleanDates(obj) {
  var keys = Object.keys(obj);
  keys.forEach(function(key) {
    if ([
      'repeatAt',
      'failedAt',
      'nextRunAt',
      'lastRunAt',
      'lastFinishedAt',
      'lockedAt'
    ].includes(key) && obj[key] instanceof Date) {
      obj[key] = obj[key]/1;
    }
  });

  return obj;
}

function cleanData(obj) {
  var keys = Object.keys(obj);
  keys.forEach(function(key) {
    if (obj[key] === undefined) {
      obj[key] = null;
    }
  });

  if (obj['lockedAt'] == null) {
    obj['lockedAt'] = new Date(1);
  }

  return cleanDates(obj);
}

function getQueryFromObject(self, obj) {
  var query;

  obj = cleanDates(obj);

  if (obj['name'] && obj['_id']) {
    query = self._collection.query(obj['name'])
      .where('_id').equals(obj['_id']);
    delete obj['name'];
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
    delete obj['name'];
  } else if (obj['_id']) {
    query = self._collection.query(obj['_id'])
      .usingIndex('IDIndex');
    delete obj['_id'];
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
    if (!err) {
      var items = !result.Items ? [result.attrs] : result.Items.map(function(obj) {
        return obj.attrs;
      });

      cb( err, items );
    } else {
      cb(err, null);
    }
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
    .attributes(['name','_id']).exec(function(error, response) {
    var promises = response.Items.map(function(value) {
      return new Promise(function(resolve) {
        self._collection.destroy({name: value.attrs.name, _id: value.attrs._id}, function (err, acc) {
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
        self._collection.destroy({name: value.attrs.name, _id: value.attrs._id}, function (err) {
          resolve(err);
        });
      });
    });

    Promise.all(promises).then(function(results) {
      var errors = [];

      results.forEach(function(result) {
        if (result) {
          errors.push(result);
        }
      });

      cb(errors);
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
  var q = this._collection.query(name);

  var keys = Object.keys(query);
  keys.forEach(function(key) {
    q.filter(key).equals(query[key]);
  });

  q.exec(function (err, response) {
    if (err || response.Items.length < 1) {
      self.insert(props, cb);
    } else {
      self.saveByID(response.Items[0].attrs._id, insertOnly, cb);
    }
  });
};

DynamoDBAdapter.prototype.insert = function(insert, cb) {
  insert = cleanData(insert);

  this._collection.create(insert, function (err, acc) {
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

DynamoDBAdapter.prototype._unlockJobs = function(jobs, done) {
  var notLocked = new Date(1);
  var self = this;

  var promises = jobs.map(function(value) {
    return new Promise(function(resolve) {
      self._collection.update({name: value.attrs.name ,_id: value.attrs._id, lockedAt: notLocked/1}, function (err, acc) {
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
    .filter('lockedAt').lte(lockDeadline/1)
    .filter('nextRunAt').lte(nextScanAt/1)
    .exec(function(error, response) {
      if (!error && response.Items.length > 0) {
        self._collection.update({name: response.Items[0].attrs.name, _id: response.Items[0].attrs._id, lockedAt: now/1 }, function(err, result) {
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
    .filter('lockedAt').equals(notLocked/1)
    .filter('nextRunAt').equals(job.attrs.nextRunAt/1)
    .filter('disabled').ne(true)
    .exec(function (error, response) {
      if (!error && response.Items.length > 0) {
        self._collection.update({name: job.attrs.name, _id: job.attrs._id, lockedAt: now/1 }, function(err, result) {
          cb(err, result.attrs);
        });
        return;
      } else {
        cb(error, null);
      }
    });
};
