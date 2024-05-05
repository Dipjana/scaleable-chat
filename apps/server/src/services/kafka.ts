import { Kafka, Producer } from "kafkajs";
import fs from 'fs';
import path from "path";
import prismaClient from "./prisma";

const kafka = new Kafka({
  brokers: ["kafka-1925e27a-dipsundarjana-a109.c.aivencloud.com:14967:14967"],
  ssl: {
    ca: [fs.readFileSync(path.resolve('./ca.pem'), 'utf-8')],
  },
  sasl: {
    username: "avnadmin",
    password: "AVNS_wGveadz8nZETGY6uJai",
    mechanism: "plain",
  },
});

let producer: null | Producer;

export async function createProducer() {
  if (producer) return producer;

  const _producer = kafka.producer();
  await _producer.connect();
  producer = _producer;
  return producer;
}

export async function produceMessage(message: string) {
  const producer = await createProducer();
  await producer.send({
    messages: [{ key: `message-${Date.now()}`, value: message }],
    topic: "MESSAGES",
  });
  return true;
}

export async function startMessageConsumer() {
const consumer = kafka.consumer({ groupId: "defualt"});
await consumer.connect();
await consumer.subscribe({ topic: "MESSAGES", fromBeginning: true });

await consumer.run({
    autoCommit: true,
    eachMessage: async ({ message, pause}) =>{
        if(!message.value) return;
        console.log(`New Message REcv..`);
try{
    await prismaClient.message.create({
        data: {
            text: message.value?.toString(),
        },
    });
} catch (err) {
    console.log("Somting is wrong");
    pause();
    setTimeout(()=>{
        consumer.resume([{ topic: "MESSAGES"}])
    }, 60 * 1000)
}
      
    }
})
}

export default kafka;
