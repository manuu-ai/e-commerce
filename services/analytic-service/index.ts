import { Kafka } from "kafkajs";
import express from "express";

const app = express();
app.use(express.json());

const kafka = new Kafka({
  clientId: "analytic-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const consumer = kafka.consumer({
  groupId: "analytic-service",
});

// Define TypeScript interfaces
interface CartItem {
  price: number;
}

interface PaymentSuccessful {
  userId: string;
  cart: CartItem[];
}

interface OrderSuccessful {
  userId: string;
  orderId: string;
}

interface EmailSuccessful {
  userId: string;
  emailId: string;
}

// Main consumer function
const run = async (): Promise<void> => {
  try {
    await consumer.connect();

    await consumer.subscribe({
      topics: [
        "payment-successful",
        "order-successful",
        "email-successful",
      ],
      fromBeginning: true,
    });

    // Listen to Kafka topics
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const value = message.value?.toString();

          if (!value) {
            console.warn(`Empty message received from topic: ${topic}`);
            return;
          }

          switch (topic) {
            case "payment-successful": {
              const { userId, cart } = JSON.parse(
                value
              ) as PaymentSuccessful;

              const total = cart
                .reduce((sum, item) => sum + item.price, 0)
                .toFixed(2);

              console.log(
                `Analytics consumer: User ${userId} paid ${total}`
              );
              break;
            }

            case "order-successful": {
              const { userId, orderId } = JSON.parse(
                value
              ) as OrderSuccessful;

              console.log(
                `Analytics consumer: Order ID ${orderId} created for user ${userId}`
              );
              break;
            }

            case "email-successful": {
              const { userId, emailId } = JSON.parse(
                value
              ) as EmailSuccessful;

              console.log(
                `Analytics consumer: Email ID ${emailId} sent to user ${userId}`
              );
              break;
            }

            default:
              console.warn(`Unhandled Kafka topic: ${topic}`);
              break;
          }
        } catch (error) {
          console.error(
            `Error processing message from topic ${topic}, partition ${partition}:`,
            error
          );
        }
      },
    });
  } catch (error) {
    console.error("Kafka consumer failed:", error);
  }
};

run();
