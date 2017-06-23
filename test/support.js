'use strict';

const 
  Sequelize = require('sequelize'),
  Config = require(__dirname + '/config/config');

const Support = {
  Sequelize,

  createSequelizeInstance(options) {
    options = options || {};
    options.dialect = this.getTestDialect();

    const config = Config[options.dialect];

    const sequelizeOptions = {
      host: options.host || config.host,
      logging: process.env.SEQ_LOG ? console.log : false,
      dialect: options.dialect,
      port: options.port || process.env.SEQ_PORT || config.port,
      pool: config.pool,
      dialectOptions: options.dialectOptions || config.dialectOptions || {}
    };

    if (process.env.DIALECT === 'postgres-native') {
      sequelizeOptions.native = true;
    }

    if (config.storage) {
      sequelizeOptions.storage = config.storage;
    }

    return this.getSequelizeInstance(config.database, config.username, config.password, sequelizeOptions);
  },

  getConnectionOptions() {
    const config = Config[this.getTestDialect()];

    delete config.pool;

    return config;
  },

  getSequelizeInstance(db, user, pass, options) {
    options = options || {};
    options.logging = false;
    options.dialect = options.dialect || this.getTestDialect();
    return new Sequelize(db, user, pass, options);
  },

  clearDatabase(sequelize) {
    return sequelize
      .getQueryInterface()
      .dropAllTables();
  },

  getSupportedDialects() {
    return ['mysql', 'postgres', 'sqlite'];
  },

  checkMatchForDialects(dialect, value, expectations) {
    if (expectations[dialect]) {
      expect(value).to.match(expectations[dialect]);
    } else {
      throw new Error('Undefined expectation for "' + dialect + '"!');
    }
  },

  getTestDialect() {
    let envDialect = process.env.DIALECT || 'mysql';

    if (envDialect === 'postgres-native') {
      envDialect = 'postgres';
    }

    if (this.getSupportedDialects().indexOf(envDialect) === -1) {
      throw new Error('The dialect you have passed is unknown. Did you really mean: ' + envDialect);
    }

    return envDialect;
  },

  getTestDialectTeaser(moduleName) {
    let dialect = this.getTestDialect();

    if (process.env.DIALECT === 'postgres-native') {
      dialect = 'postgres-native';
    }

    return '[' + dialect.toUpperCase() + '] ' + moduleName;
  },

  getTestUrl() {
    let url;
    let dialect = this.getTestDialect();
    const dbConfig = Config[dialect];

    if (dialect === 'sqlite') {
      url = 'sqlite://' + dbConfig.storage;
    } else {

      let credentials = dbConfig.username;
      if (dbConfig.password) {
        credentials += ':' + dbConfig.password;
      }

      url = dialect + '://' + credentials
      + '@' + dbConfig.host + ':' + dbConfig.port + '/' + dbConfig.database;
    }
    return url;
  },
};

Support.sequelize = Support.createSequelizeInstance();
module.exports = Support;
