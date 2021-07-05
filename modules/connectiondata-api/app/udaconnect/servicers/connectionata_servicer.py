from app.udaconnect.proto.connection_data_pb2 import SearchMessage, Person, Location, ConnectionMessage, ConnectionMessageList
from app.udaconnect.proto.connection_data_pb2_grpc import ConnectionDataServiceServicer


class ConnectionDataServicer(ConnectionDataServiceServicer):
    def FindContacts(self, request, context):
        print(request)
        resp = {
            'person_id': request.person_id,
            'start_date': request.start_date,
            'end_date': request.end_date,
            'meter': request.meters
        }

        print(resp)

        return ConnectionMessageList()
