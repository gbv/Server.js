/*! @license MIT ©2017- Jakob Voß, Verbundzentrale des GBV - VZG */
var BeaconDatasource = require('../../lib/datasources/BeaconDatasource');

var Datasource = require('../../lib/datasources/Datasource'),
    path = require('path');

var exampleBeaconUrl = 'file://' + path.join(__dirname, '../assets/test.txt');

describe('BeaconDatasource', function () {
  describe('The BeaconDatasource module', function () {
    it('should be a function', function () {
      BeaconDatasource.should.be.a('function');
    });

    it('should be a BeaconDatasource constructor', function (done) {
      var instance = new BeaconDatasource({ url: exampleBeaconUrl });
      instance.should.be.an.instanceof(BeaconDatasource);
      instance.close(done);
    });

    it('should create BeaconDatasource objects', function (done) {
      var instance = BeaconDatasource({ url: exampleBeaconUrl });
      instance.should.be.an.instanceof(BeaconDatasource);
      instance.close(done);
    });

    it('should create Datasource objects', function (done) {
      var instance = new BeaconDatasource({ url: exampleBeaconUrl });
      instance.should.be.an.instanceof(Datasource);
      instance.close(done);
    });
  });

  describe('A BeaconDatasource instance for an example Beacon file', function () {
    var datasource = new BeaconDatasource({ url: exampleBeaconUrl });
    after(function (done) { datasource.close(done); });

    itShouldExecute(datasource,
      'the empty query',
      { features: { triplePattern: true } },
      4, 4);
  });
});

function itShouldExecute(datasource, name, query, expectedResultsCount, expectedTotalCount) {
  describe('executing ' + name, function () {
    var resultsCount = 0, totalCount;
    before(function (done) {
      var result = datasource.select(query);
      result.getProperty('metadata', function (metadata) { totalCount = metadata.totalCount; });
      result.on('data', function (triple) { resultsCount++; });
      result.on('end', done);
    });

    it('should return the expected number of triples', function () {
      expect(resultsCount).to.equal(expectedResultsCount);
    });

    it('should emit the expected total number of triples', function () {
      expect(totalCount).to.equal(expectedTotalCount);
    });
  });
}
