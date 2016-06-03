'use strict';

import AWS from 'aws-sdk';
import chunk from 'chunk';
import _ from 'lodash';
import async from 'async';
import {EventEmitter} from 'events';

class Kinesis extends EventEmitter {

	constructor(credentials) {
		super();
		this._kinesis = new AWS.Kinesis(credentials);
	}

	/**
	 * Write a batch of records to a stream with an event type and timestamp
	 * @param {string} stream - the stream to write to
	 * @param {string} type - the type of the event sent
	 * @param {string} partition - useRecordProperty: use a record property, value: the shard key value or the record property name to use
	 * @param {Object[]} records - a single or an array of objects
	 * @param {string} timestamp of format YYYY-MM-DD HH:mm:ss
	 * @param {object} options
	 * @param {function} callback
	 */
	write(stream, type, partition, records, timestamp, options, callback) {

		let args = Array.prototype.slice.call(arguments);
		stream = args.shift();
		type = args.shift();
		partition = args.shift();
		records = args.shift();
		timestamp = args.shift();
		callback = args.pop();
		options = args.length > 0 ? args.shift() : {};

		const self = this;

		if (!records) {
			return callback();
		}

		//single record written to array
		if (!_.isArray(records)) {
			records = [records];
		}

		if (!records.length) {
			return callback();
		}

		self.emit('info', {
			message: 'written to stream',
			data: {stream, type, partition, count: records.length}
		});

		const kinesisRecords = _.map(records, (record) => {
			const event = {type, timestamp, data: record};
			if (options.audience) {
				event.for = options.audience;
			}
			return event;
		});

		const batches = chunk(kinesisRecords, 500);

		async.eachSeries(batches, (batch, callback) => {
			_writeBatchToStream(batch, callback);
		}, callback);

		function _writeBatchToStream(batch, callback) {

			const data = _.map(batch, (record) => {
				const partitionKey = partition.useRecordProperty ? record[partition.value] : partition.value;
				return {Data: JSON.stringify(record), PartitionKey: String(partitionKey)};
			});

			var recordParams = {
				Records: data,
				StreamName: stream
			};

			//noinspection JSUnresolvedFunction
			self._kinesis.putRecords(recordParams, function (err) {
				if (err) {
					self.emit('error', {message: 'failed to write to Kinesis stream', data: {stream}});
				}
				callback(err);
			});
		}
	}
}

export default Kinesis;