import json
import logging

from builtins import staticmethod
from kafka import KafkaProducer
from typing import Dict, List

from app import db
from app.udaconnect.models import Person

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-person-svc")


class PersonService:

    @staticmethod
    def create(person: Dict):
        PersonProducer.send_message(person)


    @staticmethod
    def retrieve(person_id: int) -> Person:
        person = db.session.query(Person).get(person_id)
        return person

    @staticmethod
    def retrieve_all() -> List[Person]:
        return db.session.query(Person).all()


TOPIC_NAME = 'person'
KAFKA_SERVER = 'kafka:9092'

kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)


class PersonProducer:
    @staticmethod
    def send_message(person):
        kafka_producer.send(TOPIC_NAME, json.dumps(person).encode())
        kafka_producer.flush(timeout=5.0)
