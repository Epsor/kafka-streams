import { KafkaStreams } from 'kafka-streams';

/**
 * @field {KafkaStreams} kafkaStreams
 */
class StreamFactory {
  /**
   * @param {Object} options         - Stream options
   * @param {String} options.groupId - Stream group identifier
   */
  constructor({ groupId, ...opts }) {
    this.kafkaStreams = new KafkaStreams({
      event_cb: true,
      'metadata.broker.list': process.env.KAFKA_HOST || 'localhost:9092',
      'group.id': groupId,
      'client.id': `${groupId}.${process.env.KAFKA_CLIENT_ID || '0'}`,
      ...opts,
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
    stream.forEach(messageIterator);

    return stream;
  }
}

export default StreamFactory;

/**
 * This callback is displayed as a global member.
 * @callback messageIterator
 * @param {String} messageContent - Message content
 * @param {Object} headers - Header content
 */
