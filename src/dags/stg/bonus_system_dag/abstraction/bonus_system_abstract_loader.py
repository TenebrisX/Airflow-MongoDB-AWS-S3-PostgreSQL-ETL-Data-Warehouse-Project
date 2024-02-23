from abc import ABC, abstractmethod
from logging import Logger
from lib import PgConnect
from stg import StgEtlSettingsRepository


class BonusAbstractLoader(ABC):
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = self.create_origin_repository(pg_origin)
        self.destination = self.create_destination_repository()
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()

    @abstractmethod
    def create_origin_repository(self, pg: PgConnect):
        """
        Factory method to create the origin repository.

        Args:
            pg (PgConnect): PostgreSQL connection object.

        Returns:
            AbstractEventsRepository: Instance of the origin repository.
        """
        pass

    @abstractmethod
    def create_destination_repository(self):
        """
        Factory method to create the destination repository.

        Returns:
            AbstractEventsRepository: Instance of the destination repository.
        """
        pass

    @abstractmethod
    def load(self):
        """
        Load method. This should be implemented by the specific loader.
        """
        pass
