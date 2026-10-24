import express from "express"
import cors from "cors"
import {Kafka} from "kafkajs"

const app = express()
app.use(cors({
    origin: "http://localhost:3000"
}))
app.use(express.json())

const kafka = new Kafka({
    clientId: "payment-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
})

const producer = kafka.producer()


const paymentSuccess = async () => {
    try {
        await producer.connect()
        console.log("producers successfully connected!")
        
    } catch (e) {
        console.log("payment service done", e)

    }
}

app.post('/payment-service', (req, res) => {
    const { cart } = req.body

    const userId = "1234"

    producer.send({
        topic: "payment-successful",
        messages: [{ value: JSON.stringify({userId, cart})}]
    })

    res.status(200).send("Payment successfully. ")
})


//api error handler
app.use((err: any, req: any, res: any, next: any) => {
    res.status(err.status ||500).send(err.message)
})

app.listen(5000, () => {
    paymentSuccess()
    console.log("Payment service is running on port 5000")
})