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

  /*
  Object.keys(Manta.prototype).forEach(m => {
    const method = Manta.prototype[m];
    if (typeof method === 'function') {
      connector.DataAccessObject[m] = method.bind(connector);
      Object.keys(method).forEach(k => {
        connector.DataAccessObject[m][k] = method[k];
      });
    }
  });
  */

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

Manta.prototype.create = function (modelName, data, callback) {
  this._directories[modelName].create(data, callback);
};

Manta.prototype.define = function (definition) {
  const modelName = definition.model.modelName;
  const pluralModelName = definition.model.pluralModelName;

  let directoryName = definition.settings.http
    ? definition.settings.http.path
    : (pluralModelName || modelName);

  this._models[modelName] = definition;

  this._directories[modelName] = new MantaDirectory(directoryName);
  this._directories[modelName].connector = this;
};
