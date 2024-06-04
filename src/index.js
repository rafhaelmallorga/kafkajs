import express from 'express';
import KafkaConfig from './config.js';
const PORT = 3000;

const server = express();
server.use(express.json());

const kafkaConfig = new KafkaConfig();

server.post('/ticket', async (req, res) => {
  try {
    console.log('fdsa');
    kafkaConfig.produce('test-topic', 'Sent By My Api', 'string');
    console.log('fasdfsadfsd');
    res.status(200).json({
      status: 'Done!',
      message: 'Message successfully send!'
    });
  } catch (err) {
    console.log(err);
  }
});

kafkaConfig.consume('test-topic', (value) => {
  console.log(value);
  console.log(value.message.value);
  console.log(value.message.value.toString());
  console.log(value.message.headers.messageType.toString());
});

server.listen(PORT, async () => {
  console.log(`Server is running on port: ${PORT}`);
});
