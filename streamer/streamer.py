#!/usr/bin/env python

"""
First part of the indexer - streamer. It listens for
new blocks in the Venom blockchain and puts the blocks ids into Kafka.
Due to performance issues on the GraphQL API side it is impossible to
fetch all data during streaming, so it is handled by fetcher.
"""

from loguru import logger
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
    producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_BROKER', 'kafka:9092'), linger_ms=5000)
    logger.info(f"Running streamer, sending data to {transactions_topic}")
    transport = WebsocketsTransport(
        url='wss://gql-testnet.venom.foundation/graphql',
        subprotocols=[WebsocketsTransport.APOLLO_SUBPROTOCOL]
    )
    async with Client(transport=transport, fetch_schema_from_transport=True) as session:
        now = int(time.time())

        subscription = gql("""
    subscription {
      messages (filter: {created_at:{gt:%s}}) {
        created_at
        created_lt
        id
        src
        dst
        msg_type_name
        value
        fwd_fee
        ihr_fee
        import_fee
        bounce
        bounced
      }
    }
        """ % str(now))
        async for result in session.subscribe(subscription):
            lag = int(time.time() - result['messages']['created_at'])
            logger.info(f"[{lag}] {result}")
            producer.send(transactions_topic, json.dumps(result).encode("utf-8"))
        producer.flush()


if __name__ == "__main__":
    asyncio.run(listener())
