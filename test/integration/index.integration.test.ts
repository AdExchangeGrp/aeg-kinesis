import Kinesis from '../../src/index';
import * as moment from 'moment-timezone';
import * as AWS from 'aws-sdk';

describe('index', async () => {

	describe('write', async () => {

		it('should return without error', async () => {

			const kinesis = new Kinesis(AWS, {
				region: 'us-west-2',
				accessKeyId: 'AKIAJNWTWN65HP3BGZ4Q',
				secretAccessKey: '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'
			});

			await kinesis.write('camp2-ci', 'test', {
				useRecordProperty: true,
				value: 'campId'
			}, [{campId: 1000}], moment.tz());

		});

	});

});
