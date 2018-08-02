from abc import abstractmethod
from typing import Dict

from jdbc_driver import AbstractJDBCImplementation


class AbstractH2JDBCDriver(AbstractJDBCImplementation):

    @abstractmethod
    def get_database_system_name(self) -> str:
        pass

    def __init__(self, properties: Dict[str, str]):
        super().__init__(properties)

    def get_java_driver_class(self) -> str:
        return "org.h2.Driver"

    def _compile_jdbc_properties(self, properties: Dict[str, str]) -> Dict[str, str]:
        h2_properties = {}
        if 'password' in properties:
            h2_properties['password'] = properties['password']
        return h2_properties


class H2ClientServerJDBCDriver(AbstractH2JDBCDriver):

    def __init__(self, properties: Dict[str, str]):
        super().__init__(properties)

    def get_database_system_name(self) -> str:
        return "H2-Client-Server"


class H2EmbeddedJDBCDriver(AbstractH2JDBCDriver):

    def __init__(self, properties: Dict[str, str]):
        super().__init__(properties)

    def get_database_system_name(self) -> str:
        return "H2-Embedded"
