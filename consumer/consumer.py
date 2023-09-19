#!/usr/bin/env python
import faust
import time
import os
from datetime import timedelta
import asyncio

TOPIC = os.environ['TOPIC']
TOPIC_PRODUCT_SUMMARY = os.environ['TOPIC_PRODUCT_SUMMARY']
INTERNAL_KAFKA_ADDR = os.environ['INTERNAL_KAFKA_ADDR']

time.sleep(15) #wait to finish starting

app = faust.App('orders', broker=INTERNAL_KAFKA_ADDR)

class Order(faust.Record, serializer='json'):
    account_id: str
    product: str
    amount: int
    price: float

orders_kafka_topic = app.topic(TOPIC, value_type=Order)
product_summary_kafka_topic = app.topic(TOPIC_PRODUCT_SUMMARY)

order_by_product = app.Table('order_count', default=int).tumbling(
    timedelta(seconds=5),
    expires=timedelta(hours=1),
)



@app.agent(channel=orders_kafka_topic, sink=[product_summary_kafka_topic])
async def process(orders: faust.Stream[Order]) -> None:
    async for order in orders.group_by(Order.product):
        order_by_product[order.product] += order.amount * order.price
        # values in this table are not concrete! access .current
        # for the value related to the time of the current event
        print("Product {}: total value: {}".format(order.product, order_by_product[order.product].current()))

        await asyncio.sleep(5)
        yield {
             'product': order.product,
             'total_value': order_by_product[order.product].current()
        }



if __name__ == '__main__':
    app.main()