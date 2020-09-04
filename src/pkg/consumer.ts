import {
  KafkaClient, Consumer, OffsetFetchRequest, ConsumerOptions, Message, Offset,
} from 'kafka-node';
import { logger } from 'juno-js';

const KAFKA_HOST = '0.0.0.0:9092';

const subscribe = (topic: string): void => {
  const client = new KafkaClient({ kafkaHost: KAFKA_HOST });
  const topics: OffsetFetchRequest[] = [{ topic, partition: 0 }];
  const options: ConsumerOptions = { autoCommit: false, fetchMaxWaitMs: 1000, fetchMaxBytes: 1024 * 1024 };

  const consumer = new Consumer(client, topics, options);

  consumer.on('error', (err: Error): void => {
    logger.error('subscribe to Kafka failed', err);
  });

  // refresh metadata, optional
  client.refreshMetadata([topic], (err: Error): void => {
    if (err) {
      logger.error('refresh metadata failed', err);
      return;
    }

    const offset = new Offset(client);

    consumer.on('message', (message: Message): void => {
      logger.info('message', message);
    });

    consumer.on(
      'offsetOutOfRange',
      // eslint-disable-next-line no-shadow
      (topic: OffsetFetchRequest): void => {
        // eslint-disable-next-line no-shadow
        offset.fetch([topic], (err: Error, offsets: any): void => {
          if (err) {
            logger.error(err);
            return;
          }
          const min = Math.min.apply(null, offsets[topic.topic][topic.partition || 5]);
          consumer.setOffset(topic.topic, topic.partition || 5, min);
        });
      }
    );
  });
};

export { subscribe };
