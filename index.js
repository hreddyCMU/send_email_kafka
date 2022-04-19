const { Kafka } = require("kafkajs");
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-2', apiVersion: "2010-12-01", accessKeyId: "AKIAVOJM5K4FXNKYTTNX", secretAccessKey:"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"  });

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
       value: message.value
     })
    var params = {
      Destination: { /* required */
        CcAddresses: [
        ],
        ToAddresses: [
          "hreddy@andrew.cmu.edu"
        ]
      },
      Source: 'hreddy@andrew.cmu.edu', /* required */
     Message: { /* required */
    Body: { /* required */
      Html: {
       Charset: "UTF-8",
       Data: "Hiii"
      },
      Text: {
       Charset: "UTF-8",
       Data: "TEXT_FORMAT_BODY"
      }
     },
     Subject: {
      Charset: 'UTF-8',
      Data: 'Test email'
     }
    },
      ReplyToAddresses: [
      ],
    };

    var sendPromise = new AWS.SES({apiVersion: '2010-12-01'}).sendEmail(params).promise();

    sendPromise.then(
      function(data) {
        console.log(data);
      }).catch(
        function(err) {
        console.error(err, err.stack);
      });

   },
 });

}
                                                                           
