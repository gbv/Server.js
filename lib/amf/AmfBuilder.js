/*! @license ©2015 Miel Vander Sande - Multimedia Lab / iMinds / Ghent University */

/** An Amf creates Approximate Membership FIlters */
var   fs = require('fs'),
  path = require('path'),
  _ = require('lodash'),
  murmurhash = require('murmurhash');

function AmfBuilder(options) {
  var amf = options.amf || {};
  var folder = path.join(__dirname, amf.dir || '../../filters');
  var type = amf.type || 'BloomFilter';

  this._datasources = options.datasources || {};
  this._probability = amf.probability || 0.001;
  this._constructor = require('./' + type);

  // setup cache
  this._cache = amf.cache || {
    get: function (key, callback) {
      fs.readFile(path.join(folder, key + '.' + type + '.json'), function (err, data) {
        if (err && err.code === 'ENOENT')
          callback(null); // file has not been found, cache entry is empty
        else if (err)
          callback(err);
        else {
          console.log('Item %s retieved from cache', key);
          callback(null, JSON.parse(data));
        }
      });
    },
    put: function (key, value, callback) {
      fs.writeFile(path.join(folder, key + '.' + type + '.json'), JSON.stringify(value), callback);
    }
  };
}

// construct AMF
AmfBuilder.prototype.build = function (query, callback) {
  // First try the cache
  var self = this,
    cache = this._cache,
    key = murmurhash.v3(JSON.stringify(_.pick(query, 'subject', 'predicate', 'object'))) + '.' + query.datasource;

  cache.get(key, function (error, filter) {
    if (error)
      callback(error);
    else if (filter)
      callback(null, filter);
    else {
      // get the data source
      var datasourceSettings = query.datasource && self._datasources[query.datasource];
      delete query.features.datasource;
      if (!datasourceSettings || !datasourceSettings.datasource.supportsQuery(query))
      //callback(new Error('Datasource ' + query.datasource + ' not valid'));
        callback(null, null); // if datasource is invalid
      else {
        self._constructor(datasourceSettings.datasource, query, self._probability, function (error, filter) {
          if (error)
            callback(error);
          else {
            // Save filter to cache
            callback(null, filter);
            cache.put(key, filter);
          }
        });
      }
    }
  });
  return true;
};

module.exports = AmfBuilder;