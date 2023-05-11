#!/usr/bin/env python

"""
First part of the indexer - streamer. It listens for 
new blocks in the Venom blockchain and puts the data into Kafka
"""

import loguru
import requests
import time
import os
import json
import asyncio
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport
from kafka import KafkaProducer

async def listener():
    transactions_topic = os.environ.get('KAFKA_TOPIC_TRANSACTIONS', 'venom_transactions')
    producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_BROKER', 'kafka:9092'))
    transport = WebsocketsTransport(
        url='wss://gql-testnet.venom.foundation/graphql',
        subprotocols=[WebsocketsTransport.APOLLO_SUBPROTOCOL]
    )
    async with Client(transport=transport, fetch_schema_from_transport=True) as session:
        now = int(time.time())

        subscription = gql("""
        subscription { transactions (filter: {now: {gt: %s}}) {
            id
            aborted
            balance_delta
            lt
            workchain_id
            out_messages {
            code_hash
            created_at
            created_lt
            body
            dst
            src
            dst_transaction {
                id
            }
            fwd_fee
            ihr_fee
            import_fee
            bounce
            bounced
            value
            status_name
            msg_type_name
            }
            now
        }
        }
        """ % str(now)) 
        async for result in session.subscribe(subscription):
            lag = int(time.time() - result['transactions']['now'])
            print(f"[{lag}] {result}")
            producer.send(transactions_topic, json.dumps(result).encode("utf-8"))
        producer.flush()
            

if __name__ == "__main__":
    asyncio.run(listener())