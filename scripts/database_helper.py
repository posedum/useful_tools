"""
This is a wrapper for database access using sqlalchemy.

"""
from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import sessionmaker


class DatabaseHelper(object):
    """
    Database Helper class.
    """

    def __init__(
            self, server_type, username, password, server, port=3306, database=None, lightweight=False, charset=''):
        """
        Initialize db helper.

        :param server_type: string - either 'mysql' or 'mssql' and indicates type of target sql server.
        :param username: string - db administrator username.
        :param password: string - db administrator password.
        :param server: string - db server IP address.
        :param port: int - db server port.
        :param database: string - db to connect to.
        :param charset: string - the database character set.
        """
        _db_login_str = self.build_connection_string(
            server_type=server_type,
            username=username,
            password=password,
            server=server,
            port=port,
            database=database,
            charset=charset
        )

        self.db_engine = create_engine(_db_login_str)

        if not lightweight:
            self.metadata = MetaData(bind=self.db_engine)
            try:
                self.db_cxn = self.db_engine.connect()
            except TypeError:
                # One retry
                self.db_cxn = self.db_engine.connect()

    def __del__(self):
        """
        Destructor close db connection.
        """
        if hasattr(self, 'db_cxn') and not self.db_cxn.closed:
            self.db_cxn.close()

    @staticmethod
    def build_connection_string(server_type, username, password, server, port=3306, database=None, charset=None):
        """
        Creates the connection string that will be based to the driver.

        :param server_type: string - either 'mysql' or 'mssql' and indicates type of target sql server.
        :param username: string - db administrator username.
        :param password: string - db administrator password.
        :param server: string - db server IP address.
        :param port: int - db server port.
        :param database: string - db to connect to.
        :param charset: string - db character set.
        :return: string - connection string.
        """
        if server_type not in ['mysql', 'mssql']:
            return None
        server_types = {
            'mysql': '{server}+pymysql://{uname}:{passwd}@{host}:{port}/{db}',
            'mssql': '{server}+pymssql://{uname}:{passwd}@{host}:{port}/{db}'
        }
        cxn_str = server_types[server_type].format(
            server=server_type,
            uname=username,
            passwd=password,
            host=server,
            port=port,
            db=database or ''
        )
        if charset:
            cxn_str += '?charset={}'.format(charset)
        return cxn_str

    def bulk_load_records(self, db_helper, table_name, records):
        """
        Bulk inserts records into table.

        :param db_helper: object - instance of DatabaseHelper.
        :param table_name: string - name of target table.
        :param records: list of dicts - records to bulk insert.
        """
        target_table = db_helper.get_table(name=table_name)
        target_table.insert().execute(records)

    def execute_call_procedure(self, proc_name, params=None):
        """
        Wrapper for executing a stored procedure.
        :param proc_name: string - stored procedure that should be called
        :param params: list - arguments that should be passed to the stored in parameters (in order).
        :return: result from stored procedure call.
        """

        connection = self.db_engine.raw_connection()
        results = None
        try:
            cursor = connection.cursor()
            if params and isinstance(params, list):
                cursor.callproc(proc_name, params)
            else:
                cursor.callproc(proc_name)
            results = list(cursor.fetchall())
            cursor.close()
            connection.commit()
        finally:
            connection.close()
        return results

    def execute_query(self, query, db_connection=None):
        """
        Wrapper to execute a query.

        :param query: query to execute.
        :param db_connection: db connection to use.
        :return:
        """
        if db_connection:
            return db_connection.execute(query)
        return self.db_cxn.execute(query)

    def get_session(self):
        """
        Create a database connection session.

        :return: object sqlalchemy Session to for current db connection.
        """
        session = sessionmaker(bind=self.db_engine)
        return session()

    def get_table(self, name):
        """
        Creates a table cursor.

        :param name:
        :return:
        """
        return Table(name, self.metadata, autoload=True)

    def print_query(self, query):
        """
        Prints the compiled query as string.

        :param query: sqlalchemy query.
        """
        print(str(query.compile(compile_kwargs={'literal_binds': True})))
