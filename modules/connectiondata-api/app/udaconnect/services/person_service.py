from functools import lru_cache
from typing import List

from app.udaconnect.infra.database import DBSession
from app.udaconnect.models import Person


session = DBSession()


class PersonService:
    @staticmethod
    def retrieve_all() -> List[Person]:
        return session.query(Person).all()

    @staticmethod
    @lru_cache(maxsize=10)
    def retrieve(person_id: int) -> Person:
        print(f"person {person_id}")
        person = session.query(Person).get(person_id)
        return person
