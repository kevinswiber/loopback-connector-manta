const { Readable } = require('stream');
const util = require('util');

const StringStream = module.exports = function (contents) {
  if (!(this instanceof StringStream)) {
    return new StringStream(contents);
  }

  Readable.call(this);

  this._contents = contents;
  this._completed = false;
}
util.inherits(StringStream, Readable);

StringStream.prototype._read = function () {
  if (!this._completed) {
    setImmediate(() => {
      this.push(new Buffer(this._contents));
      this.push(null);
    });

    this._completed = true;
  }
};
