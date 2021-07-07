# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: connection-data.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='connection-data.proto',
  package='',
  syntax='proto3',
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x15\x63onnection-data.proto\x1a\x1fgoogle/protobuf/timestamp.proto\"\x90\x01\n\rSearchMessage\x12\x11\n\tperson_id\x18\x01 \x01(\x03\x12.\n\nstart_date\x18\x02 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12,\n\x08\x65nd_date\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06meters\x18\x04 \x01(\x02\"Q\n\x06Person\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x12\n\nfirst_name\x18\x02 \x01(\t\x12\x11\n\tlast_name\x18\x03 \x01(\t\x12\x14\n\x0c\x63ompany_name\x18\x04 \x01(\t\"\x94\x01\n\x08Location\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x11\n\tperson_id\x18\x02 \x01(\x03\x12\x11\n\tlongitude\x18\x03 \x01(\t\x12\x10\n\x08latitude\x18\x04 \x01(\t\x12\x31\n\rcreation_time\x18\x05 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x11\n\twkt_shape\x18\x06 \x01(\t\"I\n\x11\x43onnectionMessage\x12\x1b\n\x08location\x18\x01 \x01(\x0b\x32\t.Location\x12\x17\n\x06person\x18\x02 \x01(\x0b\x32\x07.Person\"@\n\x15\x43onnectionMessageList\x12\'\n\x0b\x63onnections\x18\x01 \x03(\x0b\x32\x12.ConnectionMessage2O\n\x15\x43onnectionDataService\x12\x36\n\x0c\x46indContacts\x12\x0e.SearchMessage\x1a\x16.ConnectionMessageListb\x06proto3'
  ,
  dependencies=[google_dot_protobuf_dot_timestamp__pb2.DESCRIPTOR,])




_SEARCHMESSAGE = _descriptor.Descriptor(
  name='SearchMessage',
  full_name='SearchMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='person_id', full_name='SearchMessage.person_id', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='start_date', full_name='SearchMessage.start_date', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='end_date', full_name='SearchMessage.end_date', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='meters', full_name='SearchMessage.meters', index=3,
      number=4, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=59,
  serialized_end=203,
)


_PERSON = _descriptor.Descriptor(
  name='Person',
  full_name='Person',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='Person.id', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='first_name', full_name='Person.first_name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='last_name', full_name='Person.last_name', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='company_name', full_name='Person.company_name', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=205,
  serialized_end=286,
)


_LOCATION = _descriptor.Descriptor(
  name='Location',
  full_name='Location',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='Location.id', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='person_id', full_name='Location.person_id', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='longitude', full_name='Location.longitude', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='latitude', full_name='Location.latitude', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='creation_time', full_name='Location.creation_time', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='wkt_shape', full_name='Location.wkt_shape', index=5,
      number=6, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=b"".decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=289,
  serialized_end=437,
)


_CONNECTIONMESSAGE = _descriptor.Descriptor(
  name='ConnectionMessage',
  full_name='ConnectionMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='location', full_name='ConnectionMessage.location', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='person', full_name='ConnectionMessage.person', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=439,
  serialized_end=512,
)


_CONNECTIONMESSAGELIST = _descriptor.Descriptor(
  name='ConnectionMessageList',
  full_name='ConnectionMessageList',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='connections', full_name='ConnectionMessageList.connections', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=514,
  serialized_end=578,
)

_SEARCHMESSAGE.fields_by_name['start_date'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_SEARCHMESSAGE.fields_by_name['end_date'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_LOCATION.fields_by_name['creation_time'].message_type = google_dot_protobuf_dot_timestamp__pb2._TIMESTAMP
_CONNECTIONMESSAGE.fields_by_name['location'].message_type = _LOCATION
_CONNECTIONMESSAGE.fields_by_name['person'].message_type = _PERSON
_CONNECTIONMESSAGELIST.fields_by_name['connections'].message_type = _CONNECTIONMESSAGE
DESCRIPTOR.message_types_by_name['SearchMessage'] = _SEARCHMESSAGE
DESCRIPTOR.message_types_by_name['Person'] = _PERSON
DESCRIPTOR.message_types_by_name['Location'] = _LOCATION
DESCRIPTOR.message_types_by_name['ConnectionMessage'] = _CONNECTIONMESSAGE
DESCRIPTOR.message_types_by_name['ConnectionMessageList'] = _CONNECTIONMESSAGELIST
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

SearchMessage = _reflection.GeneratedProtocolMessageType('SearchMessage', (_message.Message,), {
  'DESCRIPTOR' : _SEARCHMESSAGE,
  '__module__' : 'connection_data_pb2'
  # @@protoc_insertion_point(class_scope:SearchMessage)
  })
_sym_db.RegisterMessage(SearchMessage)

Person = _reflection.GeneratedProtocolMessageType('Person', (_message.Message,), {
  'DESCRIPTOR' : _PERSON,
  '__module__' : 'connection_data_pb2'
  # @@protoc_insertion_point(class_scope:Person)
  })
_sym_db.RegisterMessage(Person)

Location = _reflection.GeneratedProtocolMessageType('Location', (_message.Message,), {
  'DESCRIPTOR' : _LOCATION,
  '__module__' : 'connection_data_pb2'
  # @@protoc_insertion_point(class_scope:Location)
  })
_sym_db.RegisterMessage(Location)

ConnectionMessage = _reflection.GeneratedProtocolMessageType('ConnectionMessage', (_message.Message,), {
  'DESCRIPTOR' : _CONNECTIONMESSAGE,
  '__module__' : 'connection_data_pb2'
  # @@protoc_insertion_point(class_scope:ConnectionMessage)
  })
_sym_db.RegisterMessage(ConnectionMessage)

ConnectionMessageList = _reflection.GeneratedProtocolMessageType('ConnectionMessageList', (_message.Message,), {
  'DESCRIPTOR' : _CONNECTIONMESSAGELIST,
  '__module__' : 'connection_data_pb2'
  # @@protoc_insertion_point(class_scope:ConnectionMessageList)
  })
_sym_db.RegisterMessage(ConnectionMessageList)



_CONNECTIONDATASERVICE = _descriptor.ServiceDescriptor(
  name='ConnectionDataService',
  full_name='ConnectionDataService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=580,
  serialized_end=659,
  methods=[
  _descriptor.MethodDescriptor(
    name='FindContacts',
    full_name='ConnectionDataService.FindContacts',
    index=0,
    containing_service=None,
    input_type=_SEARCHMESSAGE,
    output_type=_CONNECTIONMESSAGELIST,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_CONNECTIONDATASERVICE)

DESCRIPTOR.services_by_name['ConnectionDataService'] = _CONNECTIONDATASERVICE

# @@protoc_insertion_point(module_scope)
