import { KafkaClient, Producer, ProduceRequest } from 'kafka-node';
import { logger } from 'juno-js';

const KAFKA_HOST = '0.0.0.0:9092';

const publish = (topic: string, message: string): void => {
  const client = new KafkaClient({ kafkaHost: KAFKA_HOST });
  const producer = new Producer(client);

  // handle errors.
  producer.on('error', (err: Error): void => {
    logger.error('publish to Kafka failed', err);
  });

  // handler onReady event.
  producer.on('ready', (): void => {
    // refresh metadata, optional
    client.refreshMetadata([topic], (err: Error): void => {
      if (err) {
        logger.error('refresh metadata failed', err);
        return;
      }
      producer.send(
        [{ topic, messages: [message] }],
        // eslint-disable-next-line no-shadow
        (err: Error, data: ProduceRequest): void => {
          if (err) {
            logger.error('publish to Kafka failed', err);
            return;
          }
          logger.info('published to Kafka', data);
        }
      );
    });
  });
};

export { publish };
