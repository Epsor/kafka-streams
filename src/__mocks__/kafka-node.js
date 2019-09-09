export const ConsumerGroupMethods = {
  on: jest.fn(),
};

export const ConsumerGroup = jest.fn().mockImplementation(() => ConsumerGroupMethods);

module.exports = { ConsumerGroup };
