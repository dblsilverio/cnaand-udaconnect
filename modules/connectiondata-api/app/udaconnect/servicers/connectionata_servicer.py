import logging
from datetime import datetime

from app.udaconnect.proto.connection_data_pb2_grpc import ConnectionDataServiceServicer
from app.udaconnect.services.connection_service import ConnectionService

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("udaconnect-connection-servicer")


class ConnectionDataServicer(ConnectionDataServiceServicer):

    def FindContacts(self, request, context):
        """
                Finds all Person who have been within a given distance of a given Person within a date range.
                This will run rather quickly locally, but this is an expensive method and will take a bit of time to run on
                large datasets. This is by design: what are some ways or techniques to help make this data integrate more
                smoothly for a better user experience for API consumers?
        """

        params = {
            'start_date': datetime.fromtimestamp(request.start_date.seconds),
            'end_date': datetime.fromtimestamp(request.end_date.seconds),
            'person_id': int(request.person_id),
            'meters': float(request.meters)
        }

        connection_list = ConnectionService.find_contacts(**params)

        return connection_list
