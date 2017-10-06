const debug = require('debug')('loopback:connector:manta');
const StringStream = require('./string-stream');

const MantaDirectory = module.exports = function (directoryName) {
  if (!(this instanceof MantaDirectory)) {
    return new MantaDirectory(directoryName);
  }

  this.directoryName = directoryName;
}

MantaDirectory.prototype.create = function(data, callback) {
  const stringified = JSON.stringify(data);

  debug('inserting object:', stringified);

  data.id = Date.now();
  const path = `~~/stor/${this.directoryName}/${data.id}`;
  const input = new StringStream(stringified);

  const options = {
    mkdirs: true
  };

  this.connector.client.put(path, input, options, (err, response) => {
    if (err) {
      callback(err);
      return;
    }

    debug('manta response:', response.statusCode, response.headers);
    callback(null, data.id);
  });
};
