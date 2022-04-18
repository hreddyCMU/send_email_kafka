const { Kafka } = require("kafkajs");

run().then(() => console.log("Done"), err => console.log(err));

async function run() {
    const kafka = new Kafka({
        clientId: 'my-app',
        brokers: ['b-2.customer.1awnnt.c6.kafka.us-east-2.amazonaws.com:9092', 'b-1.customer.1awnnt.c6.kafka.us-east-2.amazonaws.com:9092','b-3.customer.1awnnt.c6.kafka.us-east-2.amazonaws.com:9092'],
        ssl: false
      })
    

  const consumer = kafka.consumer({ groupId: "" + Date.now() });

  await consumer.connect()
  await consumer.subscribe({ topic: 'MSKTutorialTopic', fromBeginning: true })

  await consumer.run({
   eachMessage: async ({ topic, partition, message }) => {
     console.log({
       value: message
     })
   },
 });

}
