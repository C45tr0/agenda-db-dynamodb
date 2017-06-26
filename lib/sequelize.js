var Sequelize = require('sequelize');
var Q = require('q');

function getCollection(tableName, sequelize) {
  return sequelize.define('Job', {
    _id: {
      allowNull: false,
      primaryKey: true,
      type: Sequelize.UUID,
      defaultValue: Sequelize.UUIDV4
    },
    name: {
      allowNull: false,
      type: Sequelize.STRING(256),
    },
    type: {
      allowNull: false,
      type: Sequelize.STRING(256),
    },
    priority: {
      allowNull: false,
      type: Sequelize.INTEGER(4),
      defaultValue: 0,
    },
    data: {
      type: sequelize.dialect.name !== 'postgres' ? Sequelize.JSON : Sequelize.JSONB,
    },
    repeatInterval: {
      allowNull: true,
      type: Sequelize.STRING(256),
    },
    repeatTimezone: {
      allowNull: true,
      type: Sequelize.STRING(256),
    },
    repeatAt: {
      allowNull: true,
      type: Sequelize.DATE(3),
    },
    failReason: {
      allowNull: true,
      type: Sequelize.STRING(256),
    },
    failCount: {
      allowNull: false,
      type: Sequelize.INTEGER(3).UNSIGNED,
      defaultValue: 0,
    },
    failedAt: {
      allowNull: true,
      type: Sequelize.DATE(3),
    },
    nextRunAt: {
      allowNull: true,
      type: Sequelize.DATE(3),
      defaultValue: null,
    },
    lastRunAt: {
      allowNull: true,
      type: Sequelize.DATE(3),
    },
    lastFinishedAt: {
      allowNull: true,
      type: Sequelize.DATE(3),
    },
    lastModifiedBy: {
      allowNull: true,
      type: Sequelize.STRING(256),
    },
    lockedAt: {
      allowNull: true,
      type: Sequelize.DATE(3),
      defaultValue: null,
    },
    disabled: {
      allowNull: false,
      type: Sequelize.BOOLEAN,
      defaultValue: false,
    },
  }, {
    tableName: tableName
  })
}

var SequelizeAdapter = module.exports = function(agenda, config, cb) {
  this._agenda = agenda;
  this._tableName = config.db ? config.db.collection : undefined;

  if (config.connection) {
    this.connection(config.connection, config.db ? config.db.collection : undefined, cb);
  } else if (config.db && config.db.credentials) {
    this.database(config.db.credentials, config.db.collection, cb);
  }
};

SequelizeAdapter.prototype.hasConnection = function() {
  return true;
};

// Configuration Methods

SequelizeAdapter.prototype.connection = function(sequelize, collection, cb){
  this._sequelize = sequelize;
  if (!this._collection) {
    this._collection = getCollection(collection || this._tableName || 'agendaJobs', this._sequelize);
  }
  this.db_init(cb);
  return this;
};

SequelizeAdapter.prototype.database = function(credentials, collection, cb) {
  var sequelize = new Sequelize(credentials.connectionString, credentials.dialectOptions || {});

  this._sequelize = sequelize;

  if (!this._collection) {
    this._collection = getCollection(this._tableName || collection || 'agendaJobs', this._sequelize);
  }

  this.db_init(cb);
  return this;
};

SequelizeAdapter.prototype.db_init = function(cb ){
  var self = this;

  this._collection.sync().then(() => {
    self._agenda.emit('ready');
    if (cb) {
      cb(null, self._collection);
    }
  }).catch(err => {
    self._agenda.emit('ready');
    if (cb) {
      cb(err, self._collection);
    }
  });
};

SequelizeAdapter.prototype.jobs = function(query, cb){
  /*
  if (typeof query === 'object') {
    query = getQueryFromObject(this, query);
  }
  */

  this._collection.findAll({
    where: query,
  }).then(items => {
    cb(null, items.map(item => item.get({plain: true})));
  }).catch(err => {
    cb(err, null);
  });
};

SequelizeAdapter.prototype.purge = function(definedNames, cb) {
  var self = this;

  this._collection.destroy({
    where: {
      name: {
        $notIn: definedNames,
      },
    },
  }).then(results => {
    cb(null, results);
  }).catch(err => {
    cb(err, null);
  });
};

SequelizeAdapter.prototype.cancel = function(query, cb) {
  var self = this;

  this._collection.destroy({
    where: query,
  }).then(results => {
    cb(null, results);
  }).catch(err => {
    cb(err, null);
  });
};

SequelizeAdapter.prototype.saveByID = function(id, props, cb, item = null) {
  function update(item) {
    if (Object.keys(props).length < 1) {
      return cb(null, {
        id: item._id,
        nextRunAt: item.nextRunAt
      });
    }

    Object.keys(props).forEach(key => {
      item[key] = props[key] === undefined ? null : props[key];
    }); 

    item.save().then(() => {
      cb(null, {
        id: item._id,
        nextRunAt: item.nextRunAt
      });
    }).catch(err => {
      console.log('247', err);
      cb(err, null);
    });
  }

  if (!item) {
    this._collection.findOne({
      where: {
        _id: id,
      },
    }).then(item => {
      if (!item) {
        return cb(null, null);
      }

      update(item);
    }).catch(err => {
      cb(err, null);
    })
  } else {
    update(item);
  }
};

SequelizeAdapter.prototype.saveSingle = function(name, type, props, insertOnly, cb) {
  var self = this;

  function insert() {
    var keys = Object.keys(insertOnly);
    keys.forEach(function(key) {
      props[key] = insertOnly[key];
    });

    self.insert(props, cb);
  }

  this._collection.findOne({
    where: {
      name: name,
      type: type,
    }
  }).then(item => {
    if (!item) {
      insert();
    } else {
      self.saveByID(item._id, props, cb, item);
    }
  }).catch(err => {
    console.log('292', err);
    insert();
  });
};

SequelizeAdapter.prototype.saveUnique = function(name, query, props, insertOnly, cb) {
  var self = this;

  query.name = name;

  function insert() {
    var keys = Object.keys(insertOnly);
    keys.forEach(function(key) {
      props[key] = insertOnly[key];
    });

    self.insert(props, cb);
  }

  this._collection.findOne({
    where: query,
  }).then(item => {
    if (!item) {
      insert();
    } else {
      self.saveByID(item._id, props, cb, item);
    }
  }).catch(err => {
    console.log('320', err);
    insert();
  });
};

SequelizeAdapter.prototype.insert = function(insert, cb) {

  this._collection.create(insert).then(item => {
    cb(null, {
      id: item._id,
      nextRunAt: item.nextRunAt
    });
  }).catch(err => {
    console.log('333', err);
    cb(err, null);
  });
};

SequelizeAdapter.prototype._unlockJobs = function(jobs, done) {
  var self = this;

  this._collection.update({
    lockedAt: null,
  }, {
    where: {
      _id: {
        $in: jobs,
      },
    },
  }).then(results => {
    done(null, results);
  }).catch(err => {
    done(err, null);
  });
};

SequelizeAdapter.prototype._findAndLockNextJob = function(jobName, nextScanAt, lockDeadline, cb) {
  var self = this;

  var now = new Date();

  this._collection.update({
    lockedAt: now,
  }, {
    where: {
      $or: [
        {name: jobName, lockedAt: null, nextRunAt: {$lte: nextScanAt}, disabled: { $ne: true }},
        {name: jobName, lockedAt: {$lte: lockDeadline}, disabled: { $ne: true }}
      ]
    },
    order: [
      ['nextRunAt', 'asc'],
      ['priority', 'asc'],
    ],
    limit: 1,
  }).then(results => {
    if (results[0] === 0) {
      return cb(null, null);
    }

    return this._collection.findOne({
      where: {
        name: jobName,
        lockedAt: now,
        disabled: { 
          $ne: true
        }
      }
    }).then(item => {
      if (!item) {
        return cb(null, null);
      }
      
      cb(null, item.get({plain: true}));
    })
  }).catch(err => {
    cb(err, null);
  });
};

SequelizeAdapter.prototype.lockOnTheFly = function(job, cb) {
  var self = this;

  var now = new Date();

  this._collection.update({
    lockedAt: now,
  }, {
    where: {
      _id: job.attrs._id,
      lockedAt: null,
      nextRunAt: job.attrs.nextRunAt,
      disabled: {
        $ne: true,
      },
    },
  }).then(results => {
    if (results[0] === 0) {
      return cb(null, null);
    }

    var item = job.toJSON();
    item.lockedAt = now;

    cb(null, item);
  }).catch(err => {
    console.log('441', err);
    cb(err, null);
  });
};
