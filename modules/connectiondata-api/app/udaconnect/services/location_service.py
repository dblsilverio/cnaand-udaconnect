from functools import lru_cache
from typing import List

from app.udaconnect.models import Location
from app.udaconnect.infra.database import DBSession

session = DBSession()


class LocationService:

    @staticmethod
    @lru_cache(maxsize=10)
    def fetch_locations(person_id, start_date, end_date) -> List:
        print(f"person {person_id} {start_date} {end_date}")
        locations: List = session.query(Location).filter(
            Location.person_id == person_id
        ).filter(Location.creation_time < end_date).filter(
            Location.creation_time >= start_date
        ).all()

        return locations
