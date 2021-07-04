from app.udaconnect.models import Location
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
    @api.response(202, 'Location creation accepted')
    def post(self):
        location: Location = request.get_json()

        LocationService.create(location)

        return {'status': 'accepted'}, 202


@api.route("/locations/<location_id>")
@api.param("location_id", "Unique ID for a given Location", _in="query")
class LocationResource(Resource):

    @responds(schema=LocationSchema)
    def get(self, location_id) -> Location:
        location: Location = LocationService.retrieve(location_id)
        return location
