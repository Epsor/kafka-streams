import KafkaNode from 'kafka-node';

import StreamFactory from '../streamFactory';

const defaultEnv = process.env;

describe('StreamFactory', () => {
  describe('Constructor', () => {
    beforeEach(() => {
      process.env = defaultEnv;
      KafkaNode.ConsumerGroup.mockClear();
    });

    it('should call KafkaStreams with default kafkaHost', () => {
      if (process.env.KAFKA_HOST) {
        delete process.env.KAFKA_HOST;
      }

      const stream = new StreamFactory();
      expect(stream.opts).toEqual(
        expect.objectContaining({
          kafkaHost: 'localhost:9092',
        }),
      );
    });

    it('should call KafkaStreams with default kafkaHost', () => {
      process.env.KAFKA_HOST = 'myNewHostValue';

      const stream = new StreamFactory();
      expect(stream.opts).toEqual(
        expect.objectContaining({
          kafkaHost: 'myNewHostValue',
        }),
      );
    });

    it('should call KafkaStreams with given kafkaHost', () => {
      const stream = new StreamFactory({
        kafkaHost: 'myHost',
      });
      expect(stream.opts).toEqual(
        expect.objectContaining({
          kafkaHost: 'myHost',
        }),
      );
    });

    it('should call KafkaStreams with api credentials', () => {
      const stream = new StreamFactory({
        apiKey: 'apiKey',
        apiSecret: 'apiSecret',
      });
      expect(stream.opts).toEqual(
        expect.objectContaining({
          sasl: expect.objectContaining({
            username: 'apiKey',
            password: 'apiSecret',
          }),
        }),
      );
    });
  });

  describe('on', () => {
    it('should throw when not connected', () => {
      const stream = new StreamFactory();
      expect(() => stream.on('error', () => {})).toThrow();
    });
  });

  describe('getStream', () => {
    it('should call kafkaNode.ConsumerGroup.on with sameValues', () => {
      const onMock = jest.fn();
      KafkaNode.ConsumerGroup = jest.fn(() => ({
        on: onMock,
      }));

      expect(new StreamFactory().getStream('topicName', jest.fn())).toBeTruthy();
      expect(onMock).toHaveBeenCalledTimes(1);
      expect(onMock).toHaveBeenCalledWith('message', expect.any(Function));
    });
  });

  describe('iterate', () => {
    it('shoul transfort callback', () => {
      const func = jest.fn();
      const transformedCallback = StreamFactory.iterate(func);
      const message = {
        value: 'Hello !',
      };

      transformedCallback(message);
      expect(func).toHaveBeenCalledTimes(1);
      expect(func).toHaveBeenCalledWith('Hello !');
    });
  });
});
