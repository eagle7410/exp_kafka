// @source https://hmh.engineering/experimenting-with-apache-kafka-and-nodejs-5c0604211196
const kafka = require('kafka-node');
const { KafkaClient: Client, Consumer, Message, Offset, OffsetFetchRequest, ConsumerOptions } = kafka

const kafkaHost = 'localhost:9092';
const topicTest = 'test'

function kafkaSubscribe(topic) {
	const client = new Client({ kafkaHost });
	const topics = [{ topic: topic, partition: 0 }];
	const options = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

	const consumer = new Consumer(client, topics, options);

	consumer.on('error', function(err) {
		console.log('error', err);
	});

	client.refreshMetadata(
		[topic],
		(err) => {
			const offset = new Offset(client);

			if (err) {
				throw err;
			}

			consumer.on('message', function(message) {
				// do something useful with message
				console.log(`mess ${message.offset} : ${message.value}`);
			});

			/*
			 * If consumer get `offsetOutOfRange` event, fetch data from the smallest(oldest) offset
			 */
			consumer.on(
				'offsetOutOfRange',
				(topic) => {
					offset.fetch([topic], function(err, offsets) {
						if (err) {
							return console.error(err);
						}

						const min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
						consumer.setOffset(topic.topic, topic.partition, min);
					});
				}
			);
		}
	);
}

kafkaSubscribe(topicTest)

// module.exports = kafkaSubscribe
