/*! @license MIT ©2017- Jakob Voß, Verbundzentrale des GBV - VZG */
/* A BeaconDatasource fetches data from a BEACON link dump document. */

var MemoryDatasource = require('./MemoryDatasource'),
    beacon = require('beacon-links');

var ACCEPT = 'text/plain;q=1.0';

// Creates a new BeaconDatasource
function BeaconDatasource(options) {
  if (!(this instanceof BeaconDatasource))
    return new BeaconDatasource(options);
  MemoryDatasource.call(this, options);
  this._url = options && (options.url || options.file);
}
MemoryDatasource.extend(BeaconDatasource);

// Retrieves all triples from the document
BeaconDatasource.prototype._getAllTriples = function (addTriple, done) {
  var document = this._fetch({ url: this._url, headers: { accept: ACCEPT } }, done);

  var mapper = beacon.RDFMapper({ // eslint-disable-line
    triple: function (s, p, o) { return [s, p, o]; },
    blankNode: function (id) { return '_:' + id; },
    namedNode: function (uri) { return uri; },
    literal: function (s) { return '"' + s + '"'; },
  });

  var annotation;
  document
    .pipe(beacon.Parser()) // eslint-disable-line
    .on('meta', function (meta) {
      // don't import meta triples, they belong to the dataset
      annotation = meta.ANNOTATION;
    })
    .on('data', function (link) {
      // import BEACON links with zero to two triples per link
      // this could be written cleaner with 'of' but linter complains
      var generator = mapper.linkTriples(link, annotation);
      while (true) {
        var next = generator.next();
        if (next.done) break;
        var triple = next.value;
        addTriple(triple[0], triple[1], triple[2]);
      }
    })
    .on('error', function (error) { done(error); })
    .on('end', function () { done(); });
};

module.exports = BeaconDatasource;
