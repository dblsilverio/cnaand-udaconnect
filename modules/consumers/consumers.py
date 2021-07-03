from app.udaconnect.consumers.person_consumer import PersonConsumer

KAFKA_SERVER = 'kafka:9092'


if __name__ == "__main__":
    PersonConsumer(kafka_server=KAFKA_SERVER).start()
