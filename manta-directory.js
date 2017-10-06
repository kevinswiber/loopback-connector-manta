const debug = require('debug')('loopback:connector:manta');
const StringStream = require('./string-stream');

const MantaDirectory = module.exports = function (directoryName) {
  if (!(this instanceof MantaDirectory)) {
    return new MantaDirectory(directoryName);
  }

  this.directoryName = directoryName;
}

MantaDirectory.prototype.create = function (data, callback) {
  const stringified = JSON.stringify(data);

  debug('inserting object:', stringified);

  data.id = Date.now();
  const path = `${this.directoryName}/${data.id}`;
  const input = new StringStream(stringified);

  const options = {
    mkdirs: true
  };

  this.connector.client.put(path, input, options, (err, res) => {
    if (err) {
      callback(err);
      return;
    }

    debug('manta response:', res.statusCode, res.headers);
    callback(null, data.id);
  });
};

MantaDirectory.prototype.find = function (filter, callback) {
  if (filter.where && filter.where.id) {
    return this.findById(filter.where.id, callback);
  }

  debug('listing objects');

  const path = `${this.directoryName}/`;

  this.connector.client.ls(path, (err, res) => {
    if (err) {
      callback(err);
      return;
    }

    const objs = [];
    res.on('object', obj => {
      objs.push({
        id: obj.name,
        size: obj.size,
        createdAt: obj.mtime
      });
    });

    res.on('end', () => {
      callback(null, objs);
    });
  });
};

MantaDirectory.prototype.findById = function (id, callback) {
  debug('fetching object', id);

  const path = `${this.directoryName}/${id}`;
  this.connector.client.get(path, (err, res) => {
    if (err) {
      console.log('error');
      callback(err);
      return;
    }

    const chunks = [];
    let size = 0;

    res.on('data', chunk => {
      size += chunk.length;
      chunks.push(chunk);
    });

    res.on('end', () => {
      let data = Buffer.concat(chunks, size).toString();
      data = JSON.parse(data);
      data.id = id;

      callback(null, [data]);
    });
  });
};
