#!/usr/bin/env python
import faust
import time
import os
from datetime import timedelta
import asyncio
from kafka import KafkaProducer

TOPIC_INPUT = os.environ['TOPIC_INPUT']
TOPIC_CRITICAL = os.environ['TOPIC_CRITICAL']
TOPIC_HIGH_VOLUME = os.environ['TOPIC_HIGH_VOLUME']
INTERNAL_KAFKA_ADDR = os.environ['INTERNAL_KAFKA_ADDR']
KEY = 'exception'

time.sleep(15) #wait to finish starting

app = faust.App('exception', broker=INTERNAL_KAFKA_ADDR)

class CustomException(faust.Record, serializer='json'):
    timestamp: str
    level: str
    code: str
    message: str

input_kafka_topic = app.topic(TOPIC_INPUT, value_type=CustomException)

high_volume_kafka_topic = app.topic(TOPIC_HIGH_VOLUME)
critical_kafka_topic = app.topic(TOPIC_CRITICAL)

order_by_code = app.Table('high-volumn-issues', default=int).tumbling(
    timedelta(seconds=10),
    expires=timedelta(hours=1),
)



# def publish_message(producer_instance, topic_name, key, value):
#     try:
#         key_bytes = bytes(key, encoding='utf-8')
#         value_bytes = bytes(value, encoding='utf-8')
#         producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
#         producer_instance.flush()
#         print('Message published successfully.')
#     except Exception as ex:
#         print('Exception in publishing message')
#         print(ex)

# def connect_kafka_producer():
#     _producer = None
#     try:
#         _producer = KafkaProducer(
#             bootstrap_servers=[INTERNAL_KAFKA_ADDR],
#             api_version=(0, 10))
#     except Exception as ex:
#         print('Exception while connecting Kafka')
#         print(ex)
#     return _producer

# kafka_producer = connect_kafka_producer()


# @app.agent()
# async def process2(exceptions: faust.Stream[CustomException]) -> None:
#     async for exp in exceptions.group_by(CustomException.code):
#         if exp.code not in order_by_code:
#             order_by_code[exp.code] = 1
#         else:
#             order_by_code[exp.code] += 1

#         msg = {
#             'code': exp.code,
#             'level': exp.level,
#             'message': exp.message,
#             'timestamp': exp.timestamp,
#             'count': order_by_code[exp.code].current()
#         }
#         print(msg)

#         if order_by_code[exp.code].current() > 2:
#             print("Exception with code {}: appear {} times".format(exp.code, order_by_code[exp.code].current()))
#             msg = {
#                 'code': exp.code,
#                 'level': exp.level,
#                 'message': exp.message,
#                 'timestamp': exp.timestamp,
#                 'count': order_by_code[exp.code].current()
#             }
#             high_volume_kafka_topic.send(value=msg)

#         await asyncio.sleep(10)


@app.agent(channel=input_kafka_topic)
async def process1(exceptions: faust.Stream[CustomException]) -> None:
    async for exp in exceptions:
        if exp.level == 'CRITICAL':
            await critical_kafka_topic.send(value=exp)


#https://faust.readthedocs.io/en/latest/userguide/streams.html
## use 2 agents to process data at the same time

@app.agent(channel=input_kafka_topic)
async def process(exceptions: faust.Stream[CustomException]) -> None:
    async for exp in exceptions.group_by(CustomException.code):
        order_by_code[exp.code] += 1
        if exp.code not in order_by_code:
            order_by_code[exp.code] = 1
        else:
            order_by_code[exp.code] += 1

        if order_by_code[exp.code].current() > 2:
            print("Exception with code {}: appear {} times".format(exp.code, order_by_code[exp.code].current()))
            msg = {
                'code': exp.code,
                'level': exp.level,
                'message': exp.message,
                'timestamp': exp.timestamp,
                'count': order_by_code[exp.code].current()
            }
            await high_volume_kafka_topic.send(value=msg)

        await asyncio.sleep(10)





if __name__ == '__main__':
    app.main()