import AWS from 'aws-sdk';
import chunk from 'chunk';
import _ from 'lodash';
import { EventEmitter } from 'events';
import moment from 'moment-timezone';
import Promise from 'bluebird';

class Kinesis extends EventEmitter {

	constructor (credentials) {

		super();
		this._kinesis = new AWS.Kinesis(credentials);

	}

	/**
	 * Write a batch of records to a stream with an event type and timestamp
	 * @param {string} stream - the stream to write to
	 * @param {string} type - the type of the event sent
	 * @param {string} partition - useRecordProperty: use a record property, value: the shard key value or the record property name to use
	 * @param {Object[]} records - a single or an array of objects
	 * @param {moment} timestamp - event time
	 * @param {{[published]: moment | string, [audience]: string}} [options]
	 */
	async write (stream, type, partition, records, timestamp, options) {

		options = options || {};

		const self = this;

		if (!records) {

			return;

		}

		// single record written to array
		if (!_.isArray(records)) {

			records = [records];

		}

		if (!records.length) {

			return;

		}

		self.emit('info', {
			message: 'written to stream',
			data: {stream, type, partition, count: records.length}
		});

		let published;
		let publishedIsRecordProperty = false;

		if (options.published) {

			if (moment.isMoment(options.published)) {

				published = options.published.tz('UTC').format('YYYY-MM-DD HH:mm:ss');

			} else {

				publishedIsRecordProperty = true;
				published = options.published;

			}

		} else {

			published = moment.tz('UTC').format('YYYY-MM-DD HH:mm:ss');

		}

		const kinesisRecords = _.map(records, (record) => {

			const event = {
				type,
				timestamp: timestamp.tz('UTC').format('YYYY-MM-DD HH:mm:ss'),
				published: publishedIsRecordProperty ? record[published] : published,
				data: record
			};
			if (options.audience) {

				event.for = options.audience;

			}
			return event;

		});

		const batches = chunk(kinesisRecords, 500);

		return Promise.each(batches, (batch) => {

			return _writeBatchToStream(batch);

		});

		/**
		 * Write a batch of records
		 * @param {Object[]} batch
		 * @private
		 */
		async function _writeBatchToStream (batch) {

			const data = _.map(batch, (record) => {

				const partitionKey = partition.useRecordProperty ? record[partition.value] : partition.value;
				return {Data: JSON.stringify(record), PartitionKey: String(partitionKey)};

			});

			const recordParams = {
				Records: data,
				StreamName: stream
			};

			const putRecords = Promise.promisify(self._kinesis.putRecords, {context: self._kinesis});

			return putRecords(recordParams);

		}

	}

}

export default Kinesis;
