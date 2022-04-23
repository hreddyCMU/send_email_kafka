const { Kafka } = require("kafkajs");
const nodemailer = require('nodemailer');


run().then(() => console.log("Done"), err => console.log(err));

async function run() {
    const kafka = new Kafka({
        clientId: 'my-app',
        brokers: ['b-2.msktutorialcluster.ggwq42.c23.kafka.us-east-1.amazonaws.com:9092', 'b-1.msktutorialcluster.ggwq42.c23.kafka.us-east-1.amazonaws.com:9092','b-3.msktutorialcluster.ggwq42.c23.kafka.us-east-1.amazonaws.com:9092'],
        ssl: false
      })



  const consumer = kafka.consumer({ groupId: "" + Date.now() });

  await consumer.connect()
  await consumer.subscribe({ topic: 'MSKTutorialTopic', fromBeginning: true })

  await consumer.run({
   eachMessage: async ({ topic, partition, message }) => {
     console.log({
       value: message.value
     })

const msg = Buffer.from(message.value,'hex').toString('utf8');
           const myArray = msg.split(" ");
           let transporter = nodemailer.createTransport({
            sendmail: true,
            newline: 'windows',
            logger: false
        });

        let mail = {
          from: 'hreddy@andrew.cmu.edu',
  
          // Comma separated list of recipients
          to: myArray[myArray.length-1],
  
          // Subject of the message
          subject: 'Activate your book store account',
  
          // HTML body
          html:`<html> <body> <p> Dear ${myArray[0]}</p> <p>Welcome to the Book store created by hreddy. </p> <p> Exceptionally this time we won’t ask you to click a link to activate your account. </p></body></html>`,
  
          // An array of attachments
          attachments: [
          ]
      };

      let info = await transporter.sendMail(mail);
      console.log('Message sent successfully as %s', info.messageId);


   },
 });

}
