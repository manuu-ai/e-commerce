import { Kafka } from "kafkajs";
import express from "express";

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "email-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

// Email service acts as both producer and consumer
const producer = kafka.producer();
const consumer = kafka.consumer({
  groupId: "email-service",
});

// Define the incoming order event structure
interface OrderSuccessful {
  userId: string;
  cart: {
    price: number;
    [key: string]: unknown;
  }[];
}

// Define the outgoing email event structure
interface EmailSuccessful {
  userId: string;
  emailId: string;
}

const run = async (): Promise<void> => {
  try {
    // Connect producer and consumer
    await producer.connect();
    await consumer.connect();

    // Subscribe to order events
    await consumer.subscribe({
      topic: "order-successful",
      fromBeginning: true,
    });

    // Listen for messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const value = message.value?.toString();

          if (!value) {
            console.warn(`Empty message received from ${topic}`);
            return;
          }

          const { userId, cart }: OrderSuccessful = JSON.parse(value);

          // TODO: Integrate actual email automation here
          const emailId = "987654321";

          console.log(
            `Email consumer: Email ID ${emailId} sent to user ID ${userId}`
          );

          const emailEvent: EmailSuccessful = {
            userId,
            emailId,
          };

          // Publish email success event
          await producer.send({
            topic: "email-successful",
            messages: [
              {
                value: JSON.stringify(emailEvent),
              },
            ],
          });

          console.log(
            `Published email-successful event for user ${userId}`
          );
        } catch (error) {
          console.error(
            `Failed to process message from ${topic}, partition ${partition}:`,
            error
          );
        }
      },
    });
  } catch (error) {
    console.error("Email service failed to start:", error);
  }
};

run();
