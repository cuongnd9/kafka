const { Kafka } = require('kafkajs');
const delay = require('delay');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['0.0.0.0:9092'],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
  // Producing
  await producer.connect();

  setInterval(() => {
    producer.send({
      topic: 'test-topic',
      messages: [
        { value: '💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩💩' },
      ],
    })
  }, 5000);

  // Consuming
  await consumer.connect();
  // await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
  await consumer.subscribe({ topic: 'test-topic' });

  await consumer.run({
    // @ts-ignore
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
};

run().catch(console.error);
