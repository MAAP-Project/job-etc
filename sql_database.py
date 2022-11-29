
import os
import sqlite3
import logging
from string import Template


class SQLDatabase:
    def __init__(self):
        self._connected = False
        self.db_connection = None
        self.db_cursor = None

        # database name
        self._db_name = None

        # database table names
        self._table_names = None

        # logger
        self.logger = logging.getLogger(__name__)

        return

    @property
    def isConnected(self):
        return self._connected

    @property
    def db_name(self):
        return self._db_name

    @property
    def table_names(self):
        """Determine database table names

        Parameters
        ----------

        Returns
        -------
        None
        """
        if self.isConnected:
            self.db_cursor.execute("select name from sqlite_master where type = 'table'")
            self._table_names = self.db_cursor.fetchall()

            return self._table_names
        else:
            self.logger.warning('Database not open')

        return

    def open(self, db_file, timeout=30):
        """Open sqlite database

        Parameters
        ----------
        db_file : str
            Database file

        timeout : float
            Timeout in seconds

        Returns
        -------
        None
        """
        if not os.path.isfile(db_file):
            self.logger.error('Database file %s not found.' % db_file)
            return

        try:
            self.db_connection = sqlite3.connect(database=db_file, timeout=timeout)
        except sqlite3.DatabaseError as err:
            self.logger.error('Unable to open sqlite database %s' % db_file)
            self.logger.error('sqlite error : %s' % err)
        else:
            self._db_name = os.path.splitext(db_file)[0]
            self.db_cursor = self.db_connection.cursor()
            self._connected = True

        return

    def close(self):
        """Disconnect from the database.

        Parameters
        ----------

        Returns
        -------
        None
        """
        if self.isConnected:
            self.db_connection.close()
            self._connected = False
        else:
            self.logger.warning('Database not open')

        return

    def create_db(self, db_file, timeout=30):
        """Create sqlite database

        Parameters
        ----------
        db_file : str
            Database file

        timeout : float
            Timeout in seconds

        Returns
        -------
        None
        """
        try:
            self.db_connection = sqlite3.connect(database=db_file, timeout=timeout)
        except sqlite3.DatabaseError as err:
            self.logger.error('Unable to open sqlite database %s' % db_file)
            self.logger.error('sqlite error : %s' % err)
        else:
            self._db_name = os.path.splitext(db_file)[0]
            self.db_cursor = self.db_connection.cursor()
            self._connected = True

        return

    def create_table(self, table_name, columns=""):
        """Create table with connection

        Parameters
        ----------
        table_name : str
            Table Name

        columns: str
            Column Name + data type

        Returns
        -------
        None
        """
        if self.isConnected:
            try:
                sql_template = Template('CREATE TABLE IF NOT EXISTS $table_name ($columns)')
                sql_statement = sql_template.substitute({'table_name': table_name, 'columns':columns})
                self.db_cursor.execute(sql_statement)
            except sqlite3.OperationalError as err:
                self.logger.error('Failed to create table %s' % table_name)
                self.logger.error('sqlite error : %s' % err)
        else:
            self.logger.warning('Database not open')

        return

    def drop_table(self, table_name):
        """Delete table

        Parameters
        ----------
        table_name : str
            Table Name

        Returns
        -------
        None
        """
        if self.isConnected:
            try:
                sql_template = Template('DROP TABLE IF EXISTS $table_name')
                sql_statement = sql_template.substitute({'table_name': table_name})
                self.db_cursor.execute(sql_statement)

            except sqlite3.OperationalError as err:
                self.logger.error('Failed to drop table %s' % table_name)
                self.logger.error('sqlite error : %s' % err)
        else:
            self.logger.warning('Database not open')

        return

    def table_column_name(self, table_name):
        """Get column names in the database table.

        Parameters
        ----------
        table_name : str
            Database table name

        Return
        ------
        col_names : list
            Python list of column names
        """
        if self.isConnected:
            try:
                sql_template = Template('SELECT * FROM $table_name where 1=0')
                sql_statement = sql_template.substitute({'table_name': table_name})
                self.db_cursor.execute(sql_statement)
            except sqlite3.OperationalError as err:
                self.logger.error('Failed to get column names for table %s' % table_name)
                self.logger.error('sqlite error : %s' % err)
            else:
                col_names = [description[0] for description in self.db_cursor.description]
                return col_names
        else:
            self.logger.warning('Database not open')

        return

    def get_all_rows(self, table_name):
        """Get all rows in the database table.

        Parameters
        ----------
        table_name : str
            Database table name

        Return
        ------
        rows : list
            Python list of lists containing column elements
        """
        if self.isConnected:
            try:
                sql_template = Template('SELECT * FROM $table_name')
                sql_statement = sql_template.substitute({'table_name': table_name})
                self.db_cursor.execute(sql_statement)

            except sqlite3.OperationalError as err:
                self.logger.error('Failed to get records from table %s' % table_name)
                self.logger.error('sqlite error : %s' % err)
            else:
                return self.db_cursor.fetchall()
        else:
            self.logger.warning('Database not open')

        return

    def table_query(self, table_name, columns, condition, values):
        """Get all rows in the database table.

        Parameters
        ----------
        table_name : str
            Database table name

        columns : str
            table columns to query

        condition : str
            sql conditional statement

        values : list or tuple
            List of values corresponding to conditional statement

        Return
        ------
        rows : list
            Python list of lists containing column elements
        """
        if self.isConnected:
            if isinstance(values, list) or isinstance(values, tuple):
                if condition == '':
                    sql_template = Template('SELECT $columns FROM $table_name')
                    sql_statement = sql_template.substitute({'table_name': table_name, 'columns': columns})
                else:
                    sql_template = Template('SELECT $columns FROM $table_name WHERE $condition')
                    sql_statement = sql_template.substitute({'table_name': table_name, 'columns': columns,
                                                             'condition': condition})
                self.logger.debug('SQL statement: %s' % sql_statement)

                # execute insert statement
                try:
                    self.db_cursor.execute(sql_statement, values)
                except sqlite3.OperationalError as err:
                    self.logger.error('Failed to query tables(s): %s' % table_name)
                    self.logger.error('sqlite error : %s' % err)
                else:
                    return self.db_cursor.fetchall()
            else:
                self.logger.error('Query conditional values should be list or tuple')
        else:
            self.logger.warning('Database not open')

        return list()

    def count_rows(self, table_name, columns, condition, values):
        """Return number of rows in the database table.

        Parameters
        ----------
        table_name : str
            Database table name

        columns : str
            table columns to query

        condition : str
            sql conditional statement

        values : list or tuple
            List of values corresponding to conditional statement

        Return
        ------
        rows : list
            Python list of lists containing column elements
        """
        if self.isConnected:
            if isinstance(values, list) or isinstance(values, tuple):
                if condition == '':
                    sql_template = Template('SELECT COUNT($columns) FROM $table_name')
                    sql_statement = sql_template.substitute({'table_name': table_name, 'columns': columns})
                else:
                    sql_template = Template('SELECT COUNT($columns) FROM $table_name WHERE $condition')
                    sql_statement = sql_template.substitute({'table_name': table_name, 'columns': columns,
                                                             'condition': condition})
                self.logger.debug('SQL statement: %s' % sql_statement)

                # execute insert statement
                try:
                    self.db_cursor.execute(sql_statement, values)
                except sqlite3.OperationalError as err:
                    self.logger.error('Failed to query tables(s): %s' % table_name)
                    self.logger.error('sqlite error : %s' % err)
                else:
                    return self.db_cursor.fetchall()[0][0]
            else:
                self.logger.error('Query conditional values should be list or tuple')
        else:
            self.logger.warning('Database not open')

        return 0


    def get_table_data(self, table_name):
        """Return dictionary formatted table data

        Parameters
        ----------
        table_name : str
            Database table name

        Return
        ------
        rows : list
            Python list of lists containing column elements
        """
        if self.isConnected:
            rows = self.get_all_rows(table_name)
            cols = self.table_column_name(table_name)

            data = list()
            for i, row in enumerate(rows):
                entry = {}
                for j, col in enumerate(cols):
                    entry[col] = row[j]
                data.append(entry)

            return data
        else:
            self.logger.warning('Database not open')

        return

    def insert_records(self, table_name, entries):
        """Insert values to table

        Parameters
        ----------
        table_name : str
            Table Name

        entries : dict
            entries as dictionary

        Returns
        -------
        None
        """
        if self.isConnected:
            # define sql insert template
            sql_template = Template('INSERT INTO $table_name ($column_name) VALUES ($values)')

            if isinstance(entries, dict):
                num_cols = len(entries)
                value_str = ', '.join(['?']*num_cols)

                column_str = str()
                values = list()
                for key, val in entries.items():
                    column_str += '%s, ' % key
                    values.append(val)

                # substitute values in template
                sql_statement = sql_template.substitute({'table_name': table_name, 'column_name': column_str[:-2],
                                                         'values': value_str})
                self.logger.debug('SQL statement: %s' % sql_statement)
                # execute insert statement
                try:
                    self.db_cursor.execute(sql_statement, values)
                    self.db_connection.commit()
                except sqlite3.OperationalError as err:
                    self.logger.error('Failed to insert the record')
                    self.logger.error('sqlite error : %s' % err)
            else:
                self.logger.error('Entries should be python dictionary')
        else:
            self.logger.warning('Database not open')

        return

    def table_update(self, table_name, entries, condition):
        """Update table record

        Parameters
        ----------
        table_name : str
            Database table name

        entries : dict
            Dictionary of column name and values

        condition : str
            update condition

        Returns
        -------

        """
        if self._connected:
            # define sql insert template
            sql_template = Template('UPDATE $table_name SET $column_value WHERE $condition')

            if isinstance(entries, dict):
                num_cols = len(entries)

                column_value = str()
                values = list()
                for key, val in entries.items():
                    column_value += '%s = ?, ' % key
                    values.append(val)

                # substitute values in template
                sql_statement = sql_template.substitute({'table_name': table_name, 'column_value': column_value[:-2],
                                                         'condition': condition})

                # execute insert statement
                try:
                    self.db_cursor.execute(sql_statement, values)
                    self.db_connection.commit()
                except sqlite3.OperationalError as err:
                    self.logger.error('Failed to update the record')
                    self.logger.error(sql_statement, values)
                    self.logger.error('sqlite error : %s' % err)
            else:
                self.logger.error('Entries should be python dictionary')
        else:
            self.logger.warning('Database not open')

        return

    def delete_records(self, table_name, condition, values):
        """Delete rows in the database table.
           db.delete_records("test", "Name == ?", ["DUDE"])

        Parameters
        ----------
        table_name : str
            Database table name

        condition : str
            sql conditional statement

        values : list or tuple
            List of values corresponding to conditional statement

        Return
        ------
        rows : list
            Python list of lists containing column elements
        """
        if self.isConnected:
            if isinstance(values, list) or isinstance(values, tuple):
                sql_template = Template('DELETE FROM $table_name WHERE $condition')
                sql_statement = sql_template.substitute({'table_name': table_name, 'condition': condition})
                self.logger.debug('SQL statement: %s' % sql_statement)
                
                try:
                    self.db_cursor.execute(sql_statement, values)
                except sqlite3.OperationalError as err:
                    self.logger.error('Failed to query tables(s): %s' % table_name)
                    self.logger.error('sqlite error : %s' % err)
                else:
                    self.db_connection.commit()
            else:
                self.logger.error('Query conditional values should be list or tuple')
        else:
            self.logger.warning('Database not open')

        return


def getData(db_file, table_name):
    """Get all records from a table"""
    db = SQLDatabase()

    try:
        db.open(db_file)
    except sqlite3.DatabaseError as err:
        print('Unable to open database %s' % db_file)
        print('sqlite error: %s' % err)
    else:
        records = db.table_query(table_name, columns='*', condition='', values=())
        db.close()
        return records

    # disconnect from database
    db.close()

    return
