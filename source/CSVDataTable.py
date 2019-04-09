import json
import copy
import logging



class Index:

    def __init__(self, name=None, table=None, columns=None, kind=None, loadit=None ):
        pass

    def compute_index_value(self, row):
        pass

    def add_to_index(self, row, rid):
        pass

    def delete_from_index(self, row, rid):
        pass

    def remove_from_index(self, row, rid):
        pass

    def _build(self):
        pass

    def __str__(self):
        pass

    def to_json(self):
        """
        convert the index and state to a json object
        :return: json object with index info
        """
        pass

    def from_json(self):
        pass

    def matches_index(self, template):
        pass

    def find_rows(self, tmp):
        pass



class CSVDataTable:
    """
    The database engine behaviours implementation on CSV tables
    """


    def __init__(self, table_name, column_names=None, primary_key_columns=None, loadit=False):
        """

        :param table_name: String, Name of the table, which is also name of the file in DB directory
        :param column_names: name of the columns
        :param primary_key_columns: list
        :param loadit: if true, the load method will set the values
        """
        self._table_name = table_name
        self._column_names = column_names
        self._primary_key_columns = primary_key_columns

        # dictionary containing index data structures
        self._indexes = None

        if not loadit:

            if column_names is None or table_name is None:
                raise ValueError("No table_name of column_names for table create")

            self._next_row_id = 1

            # dictionary that holds the rows
            self._rows = {}

            if primary_key_columns:
                self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")



    def get_table_name(self):
        """
        get table name
        :return:
        """
        pass

    def add_index(self, index_name, column_list, kind):
        """
        add index
        :param index_name:
        :param column_list:
        :param kind:
        :return:
        """
        pass

    def drop_index(self, index_name):
        """
        drop index
        :param index_name:
        :return:
        """
        pass

    def __str__(self):
        pass

    def _get_primary_key(self, r):
        pass

    def _get_primary_key_string(self, r):
        pass

    def _get_next_row_id(self):
        pass

    def _add_row(self, r):
        pass

    def _remove_row(self, rid):
        pass

    def import_data(self, import_data):
        """
        import data
        :param import_data:
        :return:
        """
        pass

    def save(self):
        """
        save the index to a file
        :return:
        """

        pass

    def load(self):
        """

        :return:
        """
        pass

    def get_rows_with_rids(self):
        pass

    def get_rows(self):
        pass

    def matches_template(self, row, tmp):
        pass

    def get_best_index(self, t):
        pass

    def find_by_index(self, tmp, idx):
        pass

    def find_by_scan_template(self, tmp, rows):
        pass

    def find_by_template(self, tmp, fields, use_index=True):
        pass

    def insert(self, row):
        pass

    def _get_sub_template(self, tmp, table_name):
        pass

    def load_from_rows(self, table_name, rows):
        pass




