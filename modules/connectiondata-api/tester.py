from datetime import datetime

import grpc

from google.protobuf.timestamp_pb2 import Timestamp

from app.udaconnect.proto.connection_data_pb2_grpc import ConnectionDataServiceStub
from app.udaconnect.proto.connection_data_pb2 import SearchMessage

channel = grpc.insecure_channel("localhost:5005")
stub = ConnectionDataServiceStub(channel)

t_start = datetime(year=2020, month=1, day=1)
t_end = datetime(year=2020, month=12, day=31)

ts_start = Timestamp(seconds=int(t_start.timestamp()))
ts_end = Timestamp(seconds=int(t_end.timestamp()))

print("Simple Request")

search_msg = SearchMessage(person_id=6,
                           start_date=ts_start,
                           end_date=ts_end,
                           meters=500.0)

print(f"Search Message: {search_msg}")
response = stub.FindContacts(search_msg)

print(f"Response: {response}")
