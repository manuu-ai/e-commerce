import {Kafka} from "kafkajs"

const kafka1 = new Kafka ({
    clientId: "kafka-service", 
    brokers: ["localhost:9094"]
})
const admin = kafka1.admin()

const run = async() => {
    await admin.connect()

}

run()