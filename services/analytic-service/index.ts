import {Kafka} from "kafkajs"
import express from "express"

const app = express()
app.use(express.json())

const kafka = new Kafka({
    clientId: "analytic-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"]
})

const consumer = kafka.consumer({groupId: "analytic-service"})


const run = async (): Promise <void> => {
    try {
        
        await consumer.connect()

        // const topics: string[] = ["payment-successfully, order-successfully, email-successfully"]
        
        // for (const topic of topics) { 
        // }
            
            await consumer.subscribe({
                topics: ["payment-successful", "order-successful", "email-successful"],
                fromBeginning: true,
    
            })
        

        // listen the topic
        await consumer.run({
            eachMessage: async ({topic, partition, message})=> {
             
                switch (topic) {
                    case "payment-successful":
                        {
                        const value: string | undefined = message.value?.toString()

                        const {userId, cart} = JSON.parse(value)
        
                        const total = cart.reduce((val:any, item:any) => val + item.price,  0).toFixed(2)
                        
                        console.log(`Analytic consumer: user ${userId} paid ${total}`);  }            
        
                        break;
                    
                    case "order-successful":
                        {
                        const value: string | undefined = message.value?.toString()

                        const {userId, orderId} = JSON.parse(value)

                        console.log(`Analytic consumer: Order id: ${orderId} creaded for user ${userId}`);              
                        }
                        break;
                    
                    case "email-successful":
                    {
                        const value: string | undefined = message.value?.toString()

                        const {userId, emailId} = JSON.parse(value)
                        console.log(`Analytic consumer: Email id ${emailId} sent to user ${userId}`);              
                        }
                        break;

                    default:
                        break;
                }
            }
        })

    } catch (error) {
        console.log(error);
        
    }
}

run()
