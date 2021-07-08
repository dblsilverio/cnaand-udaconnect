from datetime import datetime
from typing import List, Optional

from flask import request
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource

from app.udaconnect.models.connection import Connection
from app.udaconnect.models.location import Location
from app.udaconnect.models.person import Person
from app.udaconnect.schemas import ConnectionSchema, PersonSchema
from app.udaconnect.services.connection_service import ConnectionDataService
from app.udaconnect.services.person_service import PersonService

DATE_FORMAT = "%Y-%m-%d"

api = Namespace("UdaConnect", description="Provides person data")  # noqa


# TODO: This needs better exception handling


@api.route("/persons")
class PersonsResource(Resource):
    @accepts(schema=PersonSchema)
    @api.response(202, 'Person creation accepted')
    def post(self):
        payload = request.get_json()
        PersonService.create(payload)

        return {'status': 'accepted'}, 202

    @responds(schema=PersonSchema, many=True)
    def get(self) -> List[Person]:
        persons: List[Person] = PersonService.retrieve_all()
        return persons


@api.route("/persons/<person_id>")
@api.param("person_id", "Unique ID for a given Person", _in="query")
class PersonResource(Resource):
    @responds(schema=PersonSchema)
    def get(self, person_id) -> Person:
        person: Person = PersonService.retrieve(person_id)
        return person


@api.route("/persons/<person_id>/connection")
# @api.param("start_date", "Lower bound of date range", _in="query")
# @api.param("end_date", "Upper bound of date range", _in="query")
# @api.param("distance", "Proximity to a given user in meters", _in="query")
class ConnectionDataResource(Resource):
    @responds(schema=ConnectionSchema, many=True)
    def get(self, person_id) -> ConnectionSchema:
        start_date: datetime = datetime.strptime(
            request.args["start_date"], DATE_FORMAT
        )
        end_date: datetime = datetime.strptime(
            request.args["end_date"], DATE_FORMAT)
        distance: Optional[int] = request.args.get("distance", 5)

        results = ConnectionDataService.find_contacts(
            person_id=int(person_id),
            start_date=start_date,
            end_date=end_date,
            meters=float(distance)
        )

        connection_list: List[Connection] = [
            ConnectionDataResource.pb2_to_model(connection) for connection in results.connections
        ]

        return connection_list

    @staticmethod
    def pb2_to_model(connection) -> Connection:
        location_pb2 = connection.location
        location = Location(id=location_pb2.id, person_id=location_pb2.person_id,
                            wkt_shape=location_pb2.wkt_shape,
                            creation_time=datetime.fromtimestamp(location_pb2.creation_time.seconds))

        person_pb2 = connection.person
        person = Person(id=person_pb2.id, first_name=person_pb2.first_name,
                        last_name=person_pb2.last_name, company_name=person_pb2.company_name)

        return Connection(person=person, location=location)
