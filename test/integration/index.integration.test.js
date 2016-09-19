import Kinesis from '../../src/index';
import moment from 'moment-timezone';

describe('index', async () => {

	describe('write', async () => {

		it('should return without error', async () => {

			const kinesis = new Kinesis({
				region: 'us-west-2',
				accessKeyId: 'AKIAJNWTWN65HP3BGZ4Q',
				secretAccessKey: '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'
			});

			await kinesis.write('camp2-ci', 'test', 'campId', [{campId: 123}], moment.tz());

		});

	});

});
