import {Kafka} from 'kafkajs';

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

const run = async () => {

    const user_id = "user_125";

    const data_from_api = {order_id: "anmold",address: "niloha" };
  // Producing
  await producer.connect()
  await producer.send({
    topic: 'payment-done',
    messages: [
        {
          key: user_id,
          value: JSON.stringify(data_from_api)
        }
      ],
  })

}

run().catch(console.error)