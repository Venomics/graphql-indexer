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
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport


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
            if name == "Exchange":
                info = event['tokens']
                src_token = info['spentTokenRoot']
                dst_token = info['receiveTokenRoot']
                fees = info['fees']
                assert len(fees) == 1, fees
                fees = fees[0]
                cursor.execute("""
                        insert into exchange_event(id, created_at, sender, recipient, src_token, src_amount,
                        dst_token, dst_amount, pool_fee, beneficiary_fee, beneficiary, inserted_at)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        on conflict do nothing
                        """, (msg_id, obj['created_at'], info['sender'], info['recipient'],
                        src_token, info['spentAmount'], dst_token, info['receiveAmount'],
                        fees['pool_fee'], fees['beneficiary_fee'], fees['beneficiary']
                        ))
                logger.info(f"Insert exchange event {msg_id}, result: {cursor.rowcount}")
            elif name == "Sync":
                info = event['tokens']
                reserves = info['reserves']
                lp_supply = info['lp_supply']
                cursor.execute("""
                        insert into sync_event(id, created_at, sender, recipient,
                        reserve0, reserve1, lp_supply, inserted_at)
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                        on conflict do nothing
                        """, (msg_id, obj['created_at'], info['sender'], info['recipient'],
                        int(reserves[0]), int(reserves[1]), int(lp_supply)
                        ))
                logger.info(f"Insert sync event {msg_id}, result: {cursor.rowcount}")

                def update_token(token):
                    cursor.execute("select updated_at from tokens_info where id = %s", (token, ))
                    updated_at = cursor.fetchone()
                    if updated_at is None:
                        transport = RequestsHTTPTransport(url='https://gql-testnet.venom.foundation/graphql')
                        with Client(transport=transport, fetch_schema_from_transport=True) as session:
                            token = session.execute(gql("""
                            query {
                                ft {
                                    token(address: "%s") {
                                    address
                                    standard
                                    symbol
                                    name
                                    decimals
                                    updateTime
                                    createdAt
                                    lastTransferTimestamp
                                    rootOwner
                                    totalSupply
                                    }
                                }
                                }

                            """ % token))
                            logger.info(f"Got token info: {token}")
                            token = token['ft']['token']
                            if token is None:
                                logger.error("No data for token!")
                                return
                            cursor.execute("""
                                    insert into tokens_info(id, standard, symbol, name, decimals,
                                    created_at, root_owner, total_supply, updated_at)
                                    values (%s, %s, %s, %s, %s, %s, %s, %s, now())
                                    on conflict do nothing
                                    """, (token['address'], token['standard'], token['symbol'],
                                    token['name'], token['decimals'], token['createdAt'],
                                    token['rootOwner'], token['totalSupply']
                                    ))
                            logger.info(f"Insert token info {token}, result: {cursor.rowcount}")

                    else:
                        logger.info(f"Token already known: {updated_at}")
                update_token(src_token)
                update_token(dst_token)
                
            conn.commit()
        

if __name__ == "__main__":
    listener()