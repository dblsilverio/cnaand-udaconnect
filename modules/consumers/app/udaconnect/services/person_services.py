from app.udaconnect.infra.database import DBSession
from app.udaconnect.models.person import Person
from typing import Dict


session = DBSession()


class PersonService:
    @staticmethod
    def create(person: Dict):
        new_person = Person()
        new_person.first_name = person["first_name"]
        new_person.last_name = person["last_name"]
        new_person.company_name = person["company_name"]

        session.add(new_person)
        session.commit()
