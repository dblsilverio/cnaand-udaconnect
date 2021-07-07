import grpc
import json
import logging

from builtins import staticmethod
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer
from typing import Dict, List

from app import db
from app.udaconnect.models import Person, Connection, Location
from app.udaconnect.proto.connection_data_pb2_grpc import ConnectionDataServiceStub
from app.udaconnect.proto.connection_data_pb2 import SearchMessage

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


channel = grpc.insecure_channel("udaconnect-connectiondata-api:5005")
stub = ConnectionDataServiceStub(channel)


class ConnectionDataService:

    @staticmethod
    def find_contacts(person_id, start_date, end_date, meters):

        ts_start = Timestamp(seconds=int(start_date.timestamp()))
        ts_end = Timestamp(seconds=int(end_date.timestamp()))

        search_msg = SearchMessage(person_id=person_id,
                                   start_date=ts_start,
                                   end_date=ts_end,
                                   meters=meters)

        response = stub.FindContacts(search_msg)

        return response
