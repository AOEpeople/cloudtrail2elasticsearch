var aws  = require('aws-sdk');
var zlib = require('zlib');
var async = require('async');
var elasticsearch = require('elasticsearch');

var s3 = new aws.S3();
var client = new elasticsearch.Client({
  host: process.env.ES_HOST,
  log: 'error',
  keepAlive: false
});

exports.handler = function(event, context, callback) {
    var srcBucket = event.Records[0].s3.bucket.name;
    var srcKey = event.Records[0].s3.object.key;

    async.waterfall([
        function fetchLogFromS3(next){
            console.log('Fetching compressed log from S3...');
            s3.getObject({
               Bucket: srcBucket,
               Key: srcKey
            },
            next);
        },
        function uncompressLog(response, next){
            console.log("Uncompressing log...");
            zlib.gunzip(response.Body, next);
        },
        function publishNotifications(jsonBuffer, next) {
            console.log('Filtering log...');
            var json = jsonBuffer.toString();
            console.log('CloudTrail JSON from S3:', json);

            var records;
            try {
                records = JSON.parse(json);
            } catch (err) {
                return callback(err);
            }

            async.each(records, function(record){
              var bulk = [];
              record.forEach(function(entry){
                  entry["@timestamp"] = new Date(entry.eventTime);
                  entry["environment"] = process.env.STAGE;
              });

              bulk.push({"index": {}});
              bulk.push(record);

              client.bulk({
                index: process.env.ES_INDEXPREFIX + '-' +((new Date()).toJSON().slice(0, 10).replace(/[-T]/g, '.')),
                type: 'log',
                body: bulk
              }, function(err, resp, status) {
                if(err) {
                  console.log('Error: ', err);
                  return callback(err);
                }
                next();
              });

            });
        }

    ], function (err) {
        if (err) {
            console.error('Failed to publish notifications: ', err);
            return callback(err);
        } else {
            console.log('Successfully published all notifications.');
            return callback(null, "success");
        }
    });
};
