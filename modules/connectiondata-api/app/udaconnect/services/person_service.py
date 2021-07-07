from typing import List

from app.udaconnect.infra.database import DBSession
from app.udaconnect.models import Person


session = DBSession()


class PersonService:
    @staticmethod
    def retrieve_all() -> List[Person]:
        return session.query(Person).all()
