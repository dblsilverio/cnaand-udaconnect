import logging
import json

from kafka import KafkaProducer
from typing import Dict

from app import db
from app.udaconnect.models import Location
from app.udaconnect.schemas import LocationSchema


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-location-svc")


class LocationService:
    @staticmethod
    def retrieve(location_id) -> Location:
        location, coord_text = (
            db.session.query(Location, Location.coordinate.ST_AsText())
            .filter(Location.id == location_id)
            .one()
        )

        # Rely on database to return text form of point to reduce overhead of conversion in app code
        location.wkt_shape = coord_text
        return location

    @staticmethod
    def create(location: Dict):
        validation_results: Dict = LocationSchema().validate(location)
        if validation_results:
            logger.warning(
                f"Unexpected data format in payload: {validation_results}")
            raise Exception(f"Invalid payload: {validation_results}")

        LocationProducer.send_message(location)


TOPIC_NAME = 'location'
KAFKA_SERVER = 'kafka:9092'

kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)


class LocationProducer:
    @staticmethod
    def send_message(location):
        kafka_producer.send(TOPIC_NAME, json.dumps(location).encode())
        kafka_producer.flush(timeout=5.0)
