import {Kafka} from "kafkajs"
import express from "express"

const app = express()
app.use(express.json())

const kafka = new Kafka({
    clientId: "email-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"]

})

//order as producer and consumer
const producer = kafka.producer()
const consumer = kafka.consumer({groupId: "email-service"})


const run = async () => {
    try {
        await producer.connect()
        await consumer.connect()

        await consumer.subscribe({
            topic: "order-successful",
            fromBeginning: true
        })

        // listen the topic
        await consumer.run({
            eachMessage: async({topic, partition, message}) => {
                const value: string | undefined = message.value?.toString()

                const {userId, cart} = JSON.parse(value)

                const emailId = "987654321"
                console.log(`Email consumer: Eamil id ${emailId} Sent to user ID ${userId}`);
                

                // todo email automation
                

                await producer.send({
                    topic: "email-successful",
                    messages: [{value: JSON.stringify({userId, emailId })}

                    ]
                })

            }
        })

    } catch (error) {
        console.log(error);
        
    }
}

run()