import express from "express"
import cors from "cors"

const app = express()
app.use(cors({
    origin: "https://smart-cart-alchemy-flow.vercel.app/"
}))

app.use(express.json())

//api error handler
app.use((err: any, req: any, res: any, next: any) => {
    res.status(err.status ||500).send(err.message)
})

app.post("/payment-service", (req, res) => {
    const {cart} = req.body
    if(!cart) {
        return res.status(400).send("Cart is required")
    }
    const userId = "12345"

    //TODO: payment
    console.log("payment hit")

    //kafka

    return res.json({
        message: "Payment successful",
    })
})

app.listen(5000, () => {
    console.log("Payment service is running on port 5000")
})