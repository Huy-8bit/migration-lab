import logging
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from src.common.config import KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, CDC_CONSUMER_GROUP

logger = logging.getLogger(__name__)

class AvroKafkaConsumer:
    def __init__(self, topics: list[str], group_id: str, auto_offset_reset: str = 'earliest'):
        sr_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        self.key_deserializer = AvroDeserializer(sr_client)
        self.value_deserializer = AvroDeserializer(sr_client)
        
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,
            'session.timeout.ms': 30000,
        })
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")

    def poll_events(self, timeout: float = 1.0, max_events: int = 500) -> list[dict]:
        events = []
        while len(events) < max_events:
            msg = self.consumer.poll(timeout)
            if msg is None:
                break
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            
            key_ctx = SerializationContext(msg.topic(), MessageField.KEY)
            val_ctx = SerializationContext(msg.topic(), MessageField.VALUE)
            
            try:
                key = self.key_deserializer(msg.key(), key_ctx) if msg.key() else None
                value = self.value_deserializer(msg.value(), val_ctx) if msg.value() else None
            except Exception as e:
                logger.error(f"Deserialization error: {e}")
                continue
                
            if value is None:
                continue # tombstone
                
            events.append({
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": key,
                "value": value,
                "timestamp": msg.timestamp()
            })
            
        return events

    def commit(self):
        self.consumer.commit()
        
    def get_committed_offsets(self) -> dict:
        assignment = self.consumer.assignment()
        committed = self.consumer.committed(assignment)
        return {tp: tp.offset for tp in committed}

    def get_lag(self) -> dict:
        lag = {}
        assignment = self.consumer.assignment()
        committed = self.consumer.committed(assignment)
        
        for tp in committed:
            try:
                low, high = self.consumer.get_watermark_offsets(tp, timeout=1.0)
                if tp.offset >= 0 and high >= 0:
                    lag[f"{tp.topic}-{tp.partition}"] = high - tp.offset
            except Exception as e:
                logger.error(f"Error getting lag for {tp.topic}-{tp.partition}: {e}")
        return lag

    def close(self):
        self.consumer.close()
