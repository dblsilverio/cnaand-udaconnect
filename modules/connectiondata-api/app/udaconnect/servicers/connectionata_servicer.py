import logging
from datetime import datetime, timedelta
from google.protobuf.timestamp_pb2 import Timestamp
from sqlalchemy.sql import text
from typing import List, Dict

from app.udaconnect.infra.database import DBSession, engine
from app.udaconnect.services.person_service import PersonService
from app.udaconnect.models import Connection, Location, Person

from app.udaconnect.proto.connection_data_pb2 import SearchMessage, Person as PersonPB2, Location as LocationPB2, ConnectionMessage, ConnectionMessageList
from app.udaconnect.proto.connection_data_pb2_grpc import ConnectionDataServiceServicer


session = DBSession()


logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-connection-servicer")


class ConnectionDataServicer(ConnectionDataServiceServicer):

    @staticmethod
    def person_to_pb2(person) -> PersonPB2:
        return PersonPB2(id=person.id, first_name=person.first_name,
                         last_name=person.last_name, company_name=person.company_name)

    def FindContacts(self, request, context):
        """
                Finds all Person who have been within a given distance of a given Person within a date range.
                This will run rather quickly locally, but this is an expensive method and will take a bit of time to run on
                large datasets. This is by design: what are some ways or techniques to help make this data integrate more
                smoothly for a better user experience for API consumers?
        """

        ts_pb2_start: Timestamp = request.start_date
        ts_pb2_end: Timestamp = request.end_date

        ts_start = datetime.fromtimestamp(ts_pb2_start.seconds)
        ts_end = datetime.fromtimestamp(ts_pb2_end.seconds)

        locations: List = session.query(Location).filter(
            Location.person_id == request.person_id
        ).filter(Location.creation_time < ts_end).filter(
            Location.creation_time >= ts_start
        ).all()

        # Cache all users in memory for quick lookup
        person_map: Dict[str, Person] = {
            person.id: ConnectionDataServicer.person_to_pb2(person)
            for person in PersonService.retrieve_all()
        }

        # Prepare arguments for queries
        data = []
        for location in locations:
            data.append(
                {
                    "person_id": request.person_id,
                    "longitude": location.longitude,
                    "latitude": location.latitude,
                    "meters": request.meters,
                    "start_date": ts_start.strftime("%Y-%m-%d"),
                    "end_date": (ts_end + timedelta(days=1)).strftime("%Y-%m-%d"),
                }
            )

        query = text(
            """
        SELECT  person_id, id, ST_X(coordinate), ST_Y(coordinate), creation_time
        FROM    location
        WHERE   ST_DWithin(coordinate::geography,ST_SetSRID(ST_MakePoint(:latitude,:longitude),4326)::geography, :meters)
        AND     person_id != :person_id
        AND     TO_DATE(:start_date, 'YYYY-MM-DD') <= creation_time
        AND     TO_DATE(:end_date, 'YYYY-MM-DD') > creation_time;
        """
        )

        connection_list = ConnectionMessageList()
        result: List[ConnectionMessage] = []
        for line in tuple(data):
            for (
                    exposed_person_id,
                    location_id,
                    exposed_lat,
                    exposed_long,
                    exposed_time,
            ) in engine.execute(query, **line):
                location = LocationPB2(
                    id=location_id,
                    person_id=exposed_person_id,
                    creation_time=Timestamp(seconds=int(exposed_time.timestamp())),
                )
                location.wkt_shape = f"ST_POINT({exposed_lat} {exposed_long})"

                result.append(
                    ConnectionMessage(
                        person=ConnectionDataServicer.person_to_pb2(person_map[exposed_person_id])
                        , location=location
                    )
                )

        connection_list.connections.extend(result)

        return connection_list
