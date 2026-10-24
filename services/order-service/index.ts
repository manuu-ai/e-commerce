import { Kafka } from "kafkajs";
import express from "express";

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

// Order service acts as both producer and consumer
const producer = kafka.producer();
const consumer = kafka.consumer({
  groupId: "order-service",
});

// Incoming payment event
interface PaymentSuccessful {
  userId: string;
  cart: {
    price: number;
    [key: string]: unknown;
  }[];
}

// Outgoing order event
interface OrderSuccessful {
  userId: string;
  orderId: string;
}

const run = async (): Promise<void> => {
  try {
    // Connect producer and consumer
    await producer.connect();
    await consumer.connect();

    // Subscribe to payment events
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true,
    });

    // Listen for Kafka messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const value = message.value?.toString();

          if (!value) {
            console.warn(`Empty message received from ${topic}`);
            return;
          }

          const { userId, cart }: PaymentSuccessful = JSON.parse(value);

          // TODO: Create the actual order in the database
          const orderId = "98765";

          console.log(
            `Order consumer: Order ID ${orderId} created for user ID ${userId}`
          );

          const orderEvent: OrderSuccessful = {
            userId,
            orderId,
          };

          // Publish order-successful event
          await producer.send({
            topic: "order-successful",
            messages: [
              {
                value: JSON.stringify(orderEvent),
              },
            ],
          });

          console.log(
            `Published order-successful event for user ${userId}`
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
    console.error("Order service failed to start:", error);
  }
};
run();