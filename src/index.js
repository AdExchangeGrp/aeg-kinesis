'use strict';

import AWS from 'aws-sdk';
import chunk from 'chunk';
import _ from 'lodash';
import async from 'async';
//noinspection JSUnresolvedVariable
import {EventEmitter} from 'events';

class Kinesis extends EventEmitter {

	constructor(credentials, stream) {
		super();
		this._kinesis = new AWS.Kinesis(credentials);
		this._stream = stream;
	}

	/**
	 * Write a batch of records to a stream with an event type and timestamp
	 * @param {string} type - the type of the event sent
	 * @param {string} partitionKey - a property on the records used as a shard key
	 * @param {Object[]} records - a single or an array of objects
	 * @param {string} timestamp of format YYYY-MM-DD HH:mm:ss
	 * @param {function} callback
	 */
	write(type, partitionKey, records, timestamp, callback) {

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
			data: {stream: self._stream, type, partitionKey, count: records.length}
		});

		const kinesisRecords = _.map(records, (record) => {
			return {type, timestamp, data: record};
		});

		const batches = chunk(kinesisRecords, 500);

		async.eachSeries(batches, (batch, callback) => {
			_writeBatchToStream(batch, callback);
		}, callback);

		function _writeBatchToStream(batch, callback) {

			const data = _.map(batch, (record) => {
				return {Data: JSON.stringify(record), PartitionKey: String(record.data[partitionKey])};
			});

			var recordParams = {
				Records: data,
				StreamName: self._stream
			};

			//noinspection JSUnresolvedFunction
			this._kinesis.putRecords(recordParams, function (err) {
				if (err) {
					self.emit('error', {message: 'failed to write to Kinesis stream', data: {stream: self._stream}});
				}
				callback(err);
			});
		}
	}
}

export default Kinesis;