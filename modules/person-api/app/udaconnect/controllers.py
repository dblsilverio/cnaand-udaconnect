from datetime import datetime

from app.udaconnect.models import Person
from app.udaconnect.schemas import ConnectionSchema, PersonSchema
from app.udaconnect.services import PersonService, PersonProducer
from flask import request
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource
from typing import Any, List

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
    # @responds(schema=Any, many=True)
    def get(self, person_id) -> Any:
        # start_date: datetime = datetime.strptime(
        #     request.args["start_date"], DATE_FORMAT
        # )
        # end_date: datetime = datetime.strptime(
        #     request.args["end_date"], DATE_FORMAT)
        # distance: Optional[int] = request.args.get("distance", 5)

        # results = ConnectionService.find_contacts(
        #     person_id=person_id,
        #     start_date=start_date,
        #     end_date=end_date,
        #     meters=distance,
        # )
        return {'hello': 'world'}
