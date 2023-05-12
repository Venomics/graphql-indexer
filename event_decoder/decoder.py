#!/usr/bin/env python

"""
Third part of the indexer. It listens to the Kafka topic
with message ids and tries to decode it using https://testnetverify.venomscan.com/
"""

from loguru import logger
import requests
import time
import os
import json
import asyncio
import psycopg2
from kafka import KafkaConsumer


def listener():
    messages_decode_topic = os.environ.get('KAFKA_TOPIC_MESSAGES_DECODE_QUEUE', 'venom_messages_decode_queue')
    
    consumer = KafkaConsumer(messages_decode_topic, 
        bootstrap_servers=os.environ.get('KAFKA_BROKER', 'kafka:9092'),
        group_id=os.environ.get('KAFKA_GROUP_ID', 'event_decoder'),
        auto_offset_reset='latest'
    )
    logger.info("Starting consumer")
    conn = psycopg2.connect()
    for msg in consumer:
        obj = json.loads(msg.value.decode("utf-8"))
        msg_id = obj['id']
        logger.info(f"Got new message: {msg_id}")
        res = requests.get(f"https://testnetverify.venomscan.com/parse/message/{msg_id}")
        if res.status_code == 204:
            logger.info(f"No content for {msg_id}")
            continue
        event = res.json()
        if event['parsed_type'] != 'Event':
            logger.error(f"Unsuppoerted parsed_type: {event['parsed_type']}")
            continue
        assert event['headers'] == {}
        function_id = event['function_id']
        name = event['name']
        del event['function_id']
        del event['parsed_type']
        del event['name']
        del event['headers']

        with conn.cursor() as cursor:
            cursor.execute("""
        insert into events(id, function_id, created_at, name, data, inserted_at)
        values (%s, %s, %s, %s, %s, now())
        on conflict do nothing
        """, (msg_id, function_id, obj['created_at'], name, json.dumps(event)))
            logger.info(f"Insert event {msg_id}, result: {cursor.rowcount}")
            conn.commit()
        

if __name__ == "__main__":
    listener()