import type {
  Request,
  Response,
  NextFunction,
} from "express";
import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

const app = express();

app.use(
  cors({
    origin: "http://localhost:3000",
  })
);

app.use(express.json());

// Define cart item structure
interface CartItem {
  price: number;
  [key: string]: unknown;
}

// Define payment request body
interface PaymentRequest {
  cart: CartItem[];
}

// Initialize Kafka
const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer();

// Connect Kafka producer
const paymentSuccess = async (): Promise<void> => {
  try {
    await producer.connect();
    console.log("Producer successfully connected!");
  } catch (error) {
    console.error("Payment service connection failed:", error);
    throw error;
  }
};

// Payment API
app.post(
  "/payment-service",
  async (
    req: Request<object, string, PaymentRequest>,
    res: Response,
    next: NextFunction
  ): Promise<void> => {
    try {
      const { cart } = req.body;
      const userId = "1234";

      await producer.send({
        topic: "payment-successful",
        messages: [
          {
            value: JSON.stringify({ userId, cart }),
          },
        ],
      });

      res.status(200).send("Payment successfully processed.");
    } catch (error) {
      next(error);
    }
  }
);

// API error handler
app.use(
  (
    err: Error & { status?: number },
    req: Request,
    res: Response,
    next: NextFunction
  ): void => {
    res.status(err.status ?? 500).send(err.message || "Internal server error");
  }
);

// Start the server after connecting Kafka
const startServer = async (): Promise<void> => {
  try {
    await paymentSuccess();

    app.listen(5000, () => {
      console.log("Payment service is running on port 5000");
    });
  } catch {
    console.error("Unable to start payment service because Kafka is unavailable.");
    process.exitCode = 1;
  }
};

startServer();