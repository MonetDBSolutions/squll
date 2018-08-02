from typing import Dict

from .jdbc_driver import AbstractJDBCImplementation


class MonetDBLiteDBCDriver(AbstractJDBCImplementation):

    def get_database_system_name(self) -> str:
        return "MonetDBLite-Java"

    def __init__(self, properties: Dict[str, str]):
        super().__init__(properties)

    def get_java_driver_class(self) -> str:
        return "nl.cwi.monetdb.jdbc.MonetDriver"

    def _compile_jdbc_properties(self, properties: Dict[str, str]) -> Dict[str, str]:
        return {}
