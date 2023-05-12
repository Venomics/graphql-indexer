#!/usr/bin/env python

"""
Secoond part of the indexer - fetcher. It listens to the Kafka topic
with tx ids and fetches additional data. Also it pushes external messages for 
ABI decoding
"""

from loguru import logger
import requests
import time
import os
import json
import asyncio
import psycopg2
from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport
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
    transactions_topic_data = os.environ.get('KAFKA_TOPIC_TRANSACTIONS_DATA', 'venom_transactions_data')
    messages_decode_topic = os.environ.get('KAFKA_TOPIC_MESSAGES_DECODE_QUEUE', 'venom_messages_decode_queue')
    
    producer = KafkaProducer(bootstrap_servers=os.environ.get('KAFKA_BROKER', 'kafka:9092'))
    consumer = KafkaConsumer(transactions_topic, 
        bootstrap_servers=os.environ.get('KAFKA_BROKER', 'kafka:9092'),
        group_id=os.environ.get('KAFKA_GROUP_ID', 'indexer'),
        auto_offset_reset='latest'
    )
    transport = RequestsHTTPTransport(
        url="https://gql-testnet.venom.foundation/graphql",
        timeout=3,
        retries=10
        )
    logger.info("Starting consumer")
    conn = psycopg2.connect()
    with Client(transport=transport, fetch_schema_from_transport=True) as session:
        for msg in consumer:
            obj = json.loads(msg.value.decode("utf-8"))
            logger.info(f"Got new message: {obj}")
            tx_id = obj['transactions']['id']
            query = gql("""
    query {
        transactions (filter: {id: {eq: "%s"}}) {
            id
            account_addr
            aborted
            balance_delta
            lt
            workchain_id
            out_messages {
                id
                created_at
                created_lt
                dst
                src
                dst_transaction {
                    id
                }
                src_transaction {
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
                body_hash
            }
            in_message {
                id
                created_at
                created_lt
                dst
                src
                dst_transaction {
                    id
                }
                src_transaction {
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
                body_hash
            }
            now
            total_fees
            tr_type_name
            block {
                seq_no
                id
                shard
            }
        }
    }
        """ % str(tx_id)) 
            logger.info(f"Requesting data for tx_id {tx_id}")
            before = time.time()
            result = session.execute(query)
            time_spent = int(1000 * (time.time() - before))
            lag = int(time.time() - obj['transactions']['now'])
            logger.info(f"[{lag}s] Got result for {tx_id}: {len(json.dumps(result))} bytes, {time_spent}ms")
            producer.send(transactions_topic_data, json.dumps(result).encode("utf-8"))
            tx = result['transactions']
            if len(tx) == 0:
                logger.error(f"No transactions found for {tx_id}")
                continue
            tx = tx[0]
            # logger.info(tx)
            block = tx['block']
            
            with conn.cursor() as cursor:
                cursor.execute("""
                insert into transactions(id, account, aborted, balance_delta, lt, workchain_id, time, type, 
                total_fees, block_id, seqno, shard, inserted_at, kafka_partition, kafka_offset, kafka_timestamp)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), %s, %s, %s)
                on conflict do nothing
                """, (tx['id'], tx['account_addr'], tx['aborted'], int(tx['balance_delta'], 16), int(tx['lt'], 16), tx['workchain_id'],
                    tx['now'], tx['tr_type_name'], int(tx['total_fees'], 16), block['id'], 
                    block['seq_no'], block['shard'], msg.partition, msg.offset, msg.timestamp))
                logger.info(f"Insert transactions {tx['id']}, result: {cursor.rowcount}")

                def handle_message(msg):
                    cursor.execute("""
                insert into messages(id, src, dst, src_tx_id, dst_tx_id, created_at, created_lt, 
                fwd_fee, ihr_fee, import_fee, bounce, bounced, value, type, body_hash, inserted_at)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                on conflict(id) do update set 
                    src_tx_id = coalesce(messages.src_tx_id, excluded.src_tx_id),
                    dst_tx_id = coalesce(messages.dst_tx_id, excluded.dst_tx_id)
                """, (msg['id'], msg['src'], msg['dst'], safe_id(msg['src_transaction']), 
                    safe_id(msg['dst_transaction']), msg['created_at'], safe_hex(msg['created_lt']),
                    safe_hex(msg['fwd_fee']), safe_hex(msg['ihr_fee']), safe_hex(msg['import_fee']),
                    msg['bounce'], msg['bounced'], safe_hex(msg['value']), msg['msg_type_name'], msg['body_hash']))
                    logger.info(f"Insert message {msg['id']}, result: {cursor.rowcount}")
                    if msg['msg_type_name'] == "ExtOut":
                        producer.send(messages_decode_topic, json.dumps({
                            'id': msg['id'],
                            'msg_type_name': msg['msg_type_name']
                        }).encode("utf-8"))

                if tx['in_message'] is not None:
                    handle_message(tx['in_message'])
                for msg in tx['out_messages']:
                    handle_message(msg)
                conn.commit()
            

if __name__ == "__main__":
    listener()