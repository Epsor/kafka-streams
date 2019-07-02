import KafkaStreams from 'kafka-streams';

import StreamFactory from '../streamFactory';

const defaultEnv = process.env;

describe('StreamFactory', () => {
  describe('Contructor', () => {
    beforeEach(() => {
      process.env = defaultEnv;
      KafkaStreams.KafkaStreams.mockClear();
    });

    it('should call KafkaStreams with default kafkaHost', () => {
      if (process.env.KAFKA_HOST) {
        delete process.env.KAFKA_HOST;
      }

      expect(KafkaStreams.KafkaStreams).toHaveBeenCalledTimes(0);
      expect(new StreamFactory()).toBeTruthy();
      expect(KafkaStreams.KafkaStreams).toHaveBeenCalledTimes(1);
      expect(KafkaStreams.KafkaStreams).toHaveBeenCalledWith(
        expect.objectContaining({
          noptions: expect.objectContaining({
            'metadata.broker.list': 'localhost:9092',
          }),
        }),
      );
    });

    it('should call KafkaStreams with default kafkaHost', () => {
      process.env.KAFKA_HOST = 'myNewHostValue';

      expect(KafkaStreams.KafkaStreams).toHaveBeenCalledTimes(0);
      expect(new StreamFactory()).toBeTruthy();
      expect(KafkaStreams.KafkaStreams).toHaveBeenCalledTimes(1);
      expect(KafkaStreams.KafkaStreams).toHaveBeenCalledWith(
        expect.objectContaining({
          noptions: expect.objectContaining({
            'metadata.broker.list': 'myNewHostValue',
          }),
        }),
      );
    });
  });

  describe('on', () => {
    it('should call KafkaStreams.on with sameValues', () => {
      const callBack = jest.fn();
      const onMock = jest.fn();
      KafkaStreams.KafkaStreams = jest.fn(() => ({
        on: onMock,
      }));

      expect(onMock).toHaveBeenCalledTimes(0);
      expect(new StreamFactory().on('event', callBack)).toBeTruthy();
      expect(onMock).toHaveBeenCalledTimes(1);
      expect(onMock).toHaveBeenCalledWith('event', callBack);
    });
  });

  describe('getStream', () => {
    it('should call KafkaStreams.getKStream with sameValues', () => {
      const getKStream = jest.fn(() => ({
        forEach: jest.fn(),
        start: jest.fn(),
      }));
      KafkaStreams.KafkaStreams = jest.fn(() => ({
        getKStream,
      }));

      expect(getKStream).toHaveBeenCalledTimes(0);
      expect(new StreamFactory().getStream('topicName', jest.fn())).toBeTruthy();
      expect(getKStream).toHaveBeenCalledTimes(1);
      expect(getKStream).toHaveBeenCalledWith('topicName');
    });

    it('should call KafkaStreams.getKStream.forEach with callback', () => {
      const forEach = jest.fn();
      const start = jest.fn();
      const getKStream = jest.fn(() => ({
        forEach,
        start,
      }));
      KafkaStreams.KafkaStreams = jest.fn(() => ({
        getKStream,
      }));
      const callback = jest.fn();

      expect(forEach).toHaveBeenCalledTimes(0);
      expect(start).toHaveBeenCalledTimes(0);
      expect(new StreamFactory().getStream('topicName', callback)).toBeTruthy();
      expect(forEach).toHaveBeenCalledTimes(1);
      expect(start).toHaveBeenCalledTimes(1);
    });

    it('should call .value.toString when receiving a message from KafkaStreams.getKStream.forEach', () => {
      const toString = jest.fn(() => 'str');
      const callback = jest.fn();

      expect(callback).toHaveBeenCalledTimes(0);
      expect(toString).toHaveBeenCalledTimes(0);
      StreamFactory.iterate(callback)({
        value: {
          toString,
        },
      });
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith('str');
      expect(toString).toHaveBeenCalledTimes(1);
      expect(toString).toHaveBeenCalledWith('utf-8');
    });
  });
});
