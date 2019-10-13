// @source https://hmh.engineering/experimenting-with-apache-kafka-and-nodejs-5c0604211196
const kafka = require('kafka-node');
const { KafkaClient: Client, Producer, ProduceRequest } = kafka

const kafkaHost = 'localhost:9092';
const topicTest = 'test'

// The client connects to a Kafka broker
const client = new Client({ kafkaHost });
// The producer handles publishing messages over a topic
const producer = new Producer(client);

const publish = (topic, message) => new Promise((ok, bad) => {
	// Update metadata for the topic we'd like to publish to
	client.refreshMetadata(
		[topic],
		(err) => {
			if (err) {
				throw err;
			}

			console.log(`Sending message to ${topic}: ${message}`);
			producer.send(
				[{ topic, messages: [message] }],
				(err, result) => {
					console.log('~~ producer.send', err, result);
					if (err) return bad(err);
					return ok(result)
				}
			);
		}
	);
});

const message = () => `Message at ${(new Date()).toLocaleString()}`
producer
	.on('ready',async () => {
		console.log('\n === Producer ready. Yhoooooo === \n');

		try {
			const info = [];
			info.push( await publish(topicTest, message()) )
			info.push( await publish(topicTest, message()) )
			info.push( await publish(topicTest, message()) )

			console.log('info is ', info);

		} catch (e) {
			console.error('Error :', e);
		} finally {
			console.log('\n THE END :))) ');
			process.exit()
		}
	})
	.on('error',console.error);


