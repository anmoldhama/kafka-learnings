import {Kafka} from 'kafkajs';

import {MongoClient} from 'mongodb';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'my-app3'})



const url = "mongodb+srv://anmoldham:27FEtZxL3ZJ2TNcX@cluster0.eawxw0q.mongodb.net/";

const client = new MongoClient(url);
console.log("working");
const database = "StudyNotionDB";
let db: any;

 const createConnections = async ()=>{
    await client.connect();
    console.log("mongo connected successfully");
      db = client.db(database);
}

createConnections()
.then((res)=>console.log(res))
.catch((err)=>console.log(err));

const run = async () => {

  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'payment-done', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        const data = JSON.parse(message.value.toString());
        const userId = message.key.toString();
        console.log(`Received event for user_id ${userId}: ${JSON.stringify(data)}`);

      if(!message.value){
         return;
      }
      try {
        const collection = db.collection('users');
        await collection.updateOne(
          { user_id: userId },
          { $push: { events: data } },
          { upsert: true }
        );
        console.log(`Event stored in MongoDB for user_id ${userId}`);
      } catch (error) {
        console.error('Error saving to MongoDB', error);
      }
    },
  })
}

run().catch(console.error)