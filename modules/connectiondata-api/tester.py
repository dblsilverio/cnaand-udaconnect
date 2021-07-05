from datetime import datetime, timedelta

import grpc

from google.protobuf.timestamp_pb2 import Timestamp

from app.udaconnect.proto.connection_data_pb2_grpc import ConnectionDataServiceStub
from app.udaconnect.proto.connection_data_pb2 import SearchMessage

channel = grpc.insecure_channel("localhost:5005")
stub = ConnectionDataServiceStub(channel)

t_start = datetime.now()
t_end = t_start + timedelta(hours=1)

ts_start = Timestamp(seconds=int(t_start.timestamp()))
ts_end = Timestamp(seconds=int(t_end.timestamp()))

print("Simple Request")

search_msg = SearchMessage(person_id=123,
                           start_date=ts_start,
                           end_date=ts_end,
                           meters=500.0)

response = stub.FindContacts(search_msg)
print(response)
