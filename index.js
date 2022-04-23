const { Kafka } = require("kafkajs");
const nodemailer = require('nodemailer');


run().then(() => console.log("Done"), err => console.log(err));

async function run() {
  const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['b-2.customer.1awnnt.c6.kafka.us-east-2.amazonaws.com:9092', 'b-1.customer.1awnnt.c6.kafka.us-east-2.amazonaws.com:9092', 'b-3.customer.1awnnt.c6.kafka.us-east-2.amazonaws.com:9092'],
    ssl: false
  })



  const consumer = kafka.consumer({ groupId: "" + Date.now() });

  await consumer.connect()
  await consumer.subscribe({ topic: 'MSKTutorialTopic' })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const msg = message.value.toString();
      const myArray = msg.split(" ");
      console.log({
        value: message.value,
      });

      const [name, addr] = msg.split(" ");
      let transporter = nodemailer.createTransport({
               service: 'gmail',
        auth: {
          user: 'myjavaprograms2017@gmail.com',
          pass: 'Haripriya@37',
        },
      });

      let mail = {
        from: 'myjavaprograms2017@gmail.com',

        // Comma separated list of recipients
        to: addr,
                // Subject of the message
                subject: 'Activate your book store account',

                // HTML body
                html: `<html> <body> <p> Dear ${name}</p> <p>Welcome to the Book store created by hreddy. </p> <p> Exceptionally this time we won't ask you to click a link to activate your account. </p></body></html>`,
        
                // An array of attachments
                attachments: [
                ]
              };
                    console.log("before emial");
        let info = await transporter.sendMail(mail);
              console.log('Message sent successfully as %s', info.messageId);
        
            },
          });
        
        }
        
