'use strict';
var shipper = require('lambda-stash');

exports.handler = function(event, context, callback){
  var srcBucket = event.Records[0].s3.bucket.name;

  var config = {
    elasticsearch: {
      host: process.env.ES_HOST,
      useAWS: false
    },
    mappings:
    [
      {
        bucket: srcBucket,
        processors: [
          'decompressGzip',
          'parseJson',
          'formatCloudtrail',
          'shipElasticsearch'
        ],
        elasticsearch: {
          index: process.env.ES_INDEXPREFIX + '-' +((new Date()).toJSON().slice(0, 10).replace(/[-T]/g, '.')),
          type: 'cloudtrail'
        }
      }
    ]
  };

  shipper.handler(config, event, context, callback);

};
