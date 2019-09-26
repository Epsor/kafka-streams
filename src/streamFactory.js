import kafkaNode from 'kafka-node';

/**
 * @property {KafkaStreams} kafkaStreams
 */
class StreamFactory {
  /**
   * @param {Object} options         - Stream options
   * @param {String} options.groupId - Stream group identifier
   */
  constructor({
    groupId,
    kafkaHost = undefined,
    apiKey = undefined,
    apiSecret = undefined,
    ...opts
  } = {}) {
    this.opts = {
      kafkaHost: kafkaHost || process.env.KAFKA_HOST || 'localhost:9092',
      groupId: groupId || 'defaultGroup',
      fromOffset: 'earliest',
      /*
       * Decrease the fetchMaxBytes option to lower the number of messages by batch.
       * @TODO: Find a better solution or library (fix issue with librdkafka).
       */
      fetchMaxBytes: 1024 * 5,
      id: `${groupId}.${process.env.KAFKA_GROUP_ID || '0'}`,
      protocol: ['roundrobin'],
      ...(apiKey && apiSecret
        ? {
            sslOptions: {},
            sasl: {
              mechanism: 'plain',
              username: apiKey,
              password: apiSecret,
            },
          }
        : {}),
      ...opts,
    };
  }

  /**
   *
   * @param {String} eventName - Event name (error)
   * @param {Function} cb - Event callback
   */
  on(eventName, cb) {
    if (!this.consumer) {
      throw new Error('not connected');
    }
    this.consumer.on(eventName, cb);

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
    this.consumer = new kafkaNode.ConsumerGroup(this.opts, topicName);
    this.on('message', StreamFactory.iterate(messageIterator));

    return this.consumer;
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
