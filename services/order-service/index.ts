import {Kafka} from "kafkajs"
import express from "express"

const app = express()
app.use(express.json())

const kafka = new Kafka({
    clientId: "order-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"]
})

//order as producer and consumer
const producer = kafka.producer()
const consumer = kafka.consumer({groupId: "order-service"})


const run = async () => {
    try {
        await producer.connect()
        await consumer.connect()

        await consumer.subscribe({
            topic: "payment-successful",
            fromBeginning: true
        })

        // listen the topic
        await consumer.run({
            eachMessage: async({topic, partition, message}) => {
                const value = message.value?.toString()

                const {userId, cart} = JSON.parse(value)

                const orderId = "98765"
                console.log(`Order consumer: Order id ${orderId} Created for user Id: ${userId}`);
                

                await producer.send({
                    topic: "order-successful",
                    messages: [{value: JSON.stringify({userId, orderId: orderId })}]
                })

            }
        })

    } catch (error) {
        console.log(error);
        
    }
}

run()
