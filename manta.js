const fs = require('fs');
const manta = require('manta');
const debug = require('debug')('loopback:connector:manta');
const MantaDirectory = require('./manta-directory');

exports.initialize = (dataSource, callback) => {
  dataSource.driver = manta;

  const connector = new Manta(dataSource.settings);
  dataSource.connector = connector;
  dataSource.connector.dataSource = dataSource;

  const DataAccessObject = function () {};

  if (dataSource.constructor.DataAccessObject) {
    for (var i in dataSource.constructor.DataAccessObject) {
      DataAccessObject[i] = dataSource.constructor.DataAccessObject[i];
    }
    for (var i in dataSource.constructor.DataAccessObject.prototype) {
      DataAccessObject.prototype[i] = dataSource.constructor.DataAccessObject.prototype[i];
    }
  }

  connector.DataAccessObject = DataAccessObject;

  if (callback) {
    setImmediate(callback);
  }
};

function Manta (settings) {
  if (!(this instanceof Manta)) {
    return new Manta(settings);
  }

  this.url = settings.url;
  this.user = settings.user;
  this.subUser = settings.subUser;
  this.publicKeyID = settings.publicKeyID;
  this.rootDirectory = settings.rootDirectory || '~~/stor';

  this._models = Object.create(null);
  this._directories = Object.create(null);

  this.client = manta.createClient({
    sign: manta.sshAgentSigner({
      key: fs.readFileSync(process.env.HOME + '/.ssh/id_rsa', 'utf8'),
      keyId: this.publicKeyID,
      user: this.user
    }),
    user: this.user,
    url: this.url
  });

  debug('manta ready: %s', this.client.toString());
}

Manta.prototype.find = Manta.prototype.all = function (modelName, filter, callback) {
  this._directories[modelName].find(filter, callback);
};

Manta.prototype.create = function (modelName, data, callback) {
  this._directories[modelName].create(data, callback);
};

Manta.prototype.define = function (definition) {
  const toDisable = [
    'count',
    'createChangeStream',
    'deleteById',
    'exists',
    'findOne',
    'patchOne',
    'patchOrCreate',
    'patchOrCreateWithWhere',
    'prototype.patchAttributes',
    'prototype.updateAttributes',
    'replace',
    'replaceById',
    'replaceOrCreate',
    'update',
    'updateOrCreate',
    'updateAll',
    'upsert',
    'upsertWithWhere'
  ];

  toDisable.forEach(f => {
    definition.model.disableRemoteMethod(f, true);
  });

  const modelName = definition.model.modelName;
  const pluralModelName = definition.model.pluralModelName;

  let directoryName = definition.settings.http
    ? definition.settings.http.path
    : (pluralModelName || modelName);

  directoryName = `${this.rootDirectory}/${directoryName}`;

  this._models[modelName] = definition;

  this._directories[modelName] = new MantaDirectory(directoryName);
  this._directories[modelName].connector = this;
};
