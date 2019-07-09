import { KafkaStreams } from 'kafka-streams';

/**
 * @property {KafkaStreams} kafkaStreams
 */
class StreamFactory {
  /**
   * @param {Object} options         - Stream options
   * @param {String} options.groupId - Stream group identifier
   */
  constructor({ groupId, ...opts } = {}) {
    this.kafkaStreams = new KafkaStreams({
      noptions: {
        event_cb: true,
        'metadata.broker.list': process.env.KAFKA_HOST || 'localhost:9092',
        'group.id': groupId || 'defaultGroup',
        'client.id': `${groupId}.${process.env.KAFKA_CLIENT_ID || '0'}`,
        'fetch.min.bytes': 100,
        'fetch.message.max.bytes': 2 * 1024 * 1024,
        'queued.min.messages': 1,
        'fetch.error.backoff.ms': 100,
        'queued.max.messages.kbytes': 50,
        'fetch.wait.max.ms': 60,
        'queue.buffering.max.ms': 1000,
        'batch.num.messages': 10000,
        ...opts,
      },
      tconf: {
        'auto.offset.reset': 'earliest',
        'request.required.acks': 1,
      },
    });
  }

  /**
   *
   * @param {String} eventName - Event name (error)
   * @param {function} cb - Event callback
   */
  on(eventName, cb) {
    this.kafkaStreams.on(eventName, cb);
    return this;
  }

  /**
   * Setup the stream
   *
   * @async
   * @param {String} topicName  - The kafka topic name
   * @param {messageIterator} messageIterator - The callback that handles messages
   */
  getStream(topicName, messageIterator) {
    const stream = this.kafkaStreams.getKStream(topicName);

    stream.forEach(StreamFactory.iterate(messageIterator));

    stream.start();
    return this;
  }

  /**
   * Use from stream.forEach from this.getStream
   * @private
   */
  static iterate(messageIterator) {
    return message => messageIterator(message.value.toString('utf-8'));
  }
}

export default StreamFactory;

/**
 * This callback is displayed as a global member.
 * @callback messageIterator
 * @param {String} messageContent - Message content
 * @param {Object} headers - Header content
 */
