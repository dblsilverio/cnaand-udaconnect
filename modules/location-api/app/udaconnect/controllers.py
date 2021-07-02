from datetime import datetime

from app.udaconnect.models.person import Person
from app.udaconnect.models.person import Location
from app.udaconnect.schemas import LocationSchema
from app.udaconnect.services import LocationService
from flask import request
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource

DATE_FORMAT = "%Y-%m-%d"

api = Namespace("UdaConnect - Location API", description="Provides location data")  # noqa


# TODO: This needs better exception handling


@api.route("/locations")
class LocationListResource(Resource):
    @accepts(schema=LocationSchema)
    @responds(schema=LocationSchema)
    def post(self) -> Location:
        location: Location = LocationService.create(request.get_json())
        return location


@api.route("/locations/<location_id>")
@api.param("location_id", "Unique ID for a given Location", _in="query")
class LocationResource(Resource):

    @responds(schema=LocationSchema)
    def get(self, location_id) -> Location:
        location: Location = LocationService.retrieve(location_id)
        return location
