const { Kafka } = require("kafkajs");

run().then(() => console.log("Done"), err => console.log(err));

async function run() {
  const kafka = new Kafka({ brokers: ["localhost:9092"] });

  const consumer = kafka.consumer({ groupId: "" + Date.now() });

  await consumer.connect();

  await consumer.subscribe({ topic: "quickstart-events", fromBeginning: true });
  await consumer.run({ 
    eachMessage: async (data) => {
      console.log(data);
    }
  });

}