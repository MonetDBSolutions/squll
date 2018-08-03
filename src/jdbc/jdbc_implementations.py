from abc import abstractmethod
from typing import Dict


class AbstractJDBCImplementation:

    def __init__(self, conf_properties: Dict[str, str]):
        config_keys = ['uri', 'jars', 'user']
        for c in config_keys:
            if c not in conf_properties:
                print('Configuration key "%s" not set in configuration file for target "%s"' % (
                        c, self.get_database_system_name()))
                exit(-1)
        self.uri = conf_properties['uri']
        self.jars = [x.strip() for x in conf_properties['jars'].split(',')]
        self.properties = {}
        for k in self.get_supported_properties():
            if k in conf_properties:
                self.properties[k] = conf_properties[k]
        if 'user' in conf_properties:
            self.properties['user'] = conf_properties['user']
        if 'password' in conf_properties:
            self.properties['password'] = conf_properties['password']

    @abstractmethod
    def get_database_system_name(self) -> str:
        pass

    @abstractmethod
    def get_java_driver_class(self) -> str:
        pass

    @abstractmethod
    def get_supported_properties(self) -> [str]:
        pass

    def get_jdbc_uri(self) -> str:
        return self.uri

    def get_jdbc_jars_path(self) -> [str]:
        return self.jars

    def get_jdbc_properties(self) -> Dict[str, str]:
        return self.properties


class ApacheDerbyJDBCDriver(AbstractJDBCImplementation):

    def __init__(self, conf_properties: Dict[str, str]):
        super().__init__(conf_properties)

    def get_database_system_name(self) -> str:
        return "Apache Derby"

    def get_java_driver_class(self) -> str:
        return "org.apache.derby.jdbc.EmbeddedDriver"

    def get_supported_properties(self) -> [str]:
        return ['password', 'create', 'logDevice']


class H2JDBCDriver(AbstractJDBCImplementation):

    def __init__(self, conf_properties: Dict[str, str]):
        super().__init__(conf_properties)

    def get_database_system_name(self) -> str:
        return "H2"

    def get_java_driver_class(self) -> str:
        return "org.h2.Driver"

    def get_supported_properties(self) -> [str]:
        return ['password']


class MonetDBLiteDBCDriver(AbstractJDBCImplementation):

    def get_database_system_name(self) -> str:
        return "MonetDBLite-Java"

    def __init__(self, conf_properties: Dict[str, str]):
        super().__init__(conf_properties)

    def get_java_driver_class(self) -> str:
        return "nl.cwi.monetdb.jdbc.MonetDriver"

    def get_supported_properties(self) -> [str]:
        return []
