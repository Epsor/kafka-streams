export const KafkaStreamsMethods = {
  on: jest.fn(),
  getKStream: jest.fn(),
};

export const KafkaStreams = jest.fn().mockImplementation(() => KafkaStreamsMethods);

module.exports = { KafkaStreams };
