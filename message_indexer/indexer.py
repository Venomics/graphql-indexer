#!/usr/bin/env python

"""
Secoond part of the indexer. It listens to the Kafka topic
with messages data. Also it pushes external messages for 
ABI decoding.
"""

from loguru import logger
import requests
import time
import os
import json
import asyncio
import psycopg2
from kafka import KafkaProducer, KafkaConsumer

def safe_id(o):
    if o is None:
        return None
    return o['id']
def safe_hex(o):
    if o is None:
        return None
    else:
        return int(o, 16)

def listener():
    transactions_topic = os.environ.get('KAFKA_TOPIC_TRANSACTIONS', 'venom_transactions')
    messages_decode_topic = os.environ.get('KAFKA_TOPIC_MESSAGES_DECODE_QUEUE', 'venom_messages_decode_queue')
    
    producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_BROKER', 'kafka:9092'))
    consumer = KafkaConsumer(transactions_topic, 
        bootstrap_servers=os.environ.get('KAFKA_BROKER', 'kafka:9092'),
        group_id=os.environ.get('KAFKA_GROUP_ID', 'msg_indexer'),
        auto_offset_reset='latest'
    )
    logger.info("Starting consumer")
    conn = psycopg2.connect()
    for msg in consumer:
        obj = json.loads(msg.value.decode("utf-8"))
        logger.info(f"Got new message: {obj}")
        msg = obj['messages']
        
        with conn.cursor() as cursor:
            cursor.execute("""
        insert into messages(id, src, dst, created_at, created_lt, 
        fwd_fee, ihr_fee, import_fee, bounce, bounced, value, type, inserted_at)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        on conflict do nothing
        """, (msg['id'], msg['src'], msg['dst'], msg['created_at'], safe_hex(msg['created_lt']),
            safe_hex(msg['fwd_fee']), safe_hex(msg['ihr_fee']), safe_hex(msg['import_fee']),
            msg['bounce'], msg['bounced'], safe_hex(msg['value']), msg['msg_type_name']))
            logger.info(f"Insert message {msg['id']}, result: {cursor.rowcount}")
            if msg['msg_type_name'] == "ExtOut":
                producer.send(messages_decode_topic, json.dumps({
                    'id': msg['id'],
                    'msg_type_name': msg['msg_type_name'],
                    'created_at': msg['created_at']
                }).encode("utf-8"))
            conn.commit()
        

if __name__ == "__main__":
    listener()