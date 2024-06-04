import { Kafka } from 'kafkajs';

class KafkaConfig {
  constructor() {
    this.kafka = new Kafka({
      clientId: 'my-app',
      brokers: ['localhost:9092', 'localhost:9092']
    });
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: 'integrator-consumer-id' });
  }

  async produce(topic, messages, messageType) {
    try {
      await this.producer.connect();
      await this.producer.send({
        topic: topic,
        messages: [{ value: messages, headers: { messageType: messageType } }]
      });
    } catch (err) {
      console.log(err);
    } finally {
      await this.producer.disconnect();
    }
  }

  async consume(topic, callBack) {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: topic, fromBeginning: true });
      await this.consumer.run({
        eachMessage: async (param) => {
          //console.log(topic);
          //console.log(partition);
          //console.log({
          //  value: message.value.toString()
          // });
          callBack(param);
        }
      });
    } catch (err) {
      console.log(err);
    }
  }
}

export default KafkaConfig;
