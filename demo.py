import asyncio
import os
import time
from fastapi import FastAPI
from pydantic import BaseModel
import joblite
from joblite_fastapi.joblite_fastapi import JobliteApp

app = FastAPI()

if os.path.exists("app.db"):
    os.remove("app.db")

db = joblite.open("app.db")
jl = JobliteApp(app, db)

emails_sent = []


@jl.task("emails", visibility_timeout_s=300)
async def send_email(payload):
    print(f"Sending email to {payload['to']} with body: {payload['body']}")
    emails_sent.append(payload)


class Order(BaseModel):
    user_id: int
    amount: float
    email: str


@app.on_event("startup")
def setup_db():
    with db.transaction() as tx:
        tx.execute(
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)"
        )


@app.post("/orders")
async def create_order(order: Order):
    with db.transaction() as tx:
        tx.execute(
            "INSERT INTO orders (user_id, amount) VALUES (?, ?)",
            [order.user_id, order.amount],
        )
        queue = jl.tasks["emails"]["queue"]
        queue.enqueue({"to": order.email, "body": f"Receipt for {order.amount}"}, tx=tx)
    return {"ok": True}


@app.get("/stats")
async def get_stats():
    with db.transaction() as tx:
        rows = tx.query("SELECT COUNT(*) as c FROM orders", [])
        return {"orders": rows[0]["c"], "emails_sent": len(emails_sent)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
