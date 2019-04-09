import json
import copy
import logging



class Index:

    def __init__(self, index_name, index_columns, kind):
        self._index_name = index_name
        self._index_columns = index_columns
        self._kind = kind

        self._index_data = None  # dictionary that contains the index data

    def compute_key(self, row):

        key_value = [row[k] for k in self._index_columns]
        key_value = "_".join(key_value)
        return key_value


    def add_to_index(self, row, rid):
        if self._index_data is None:
            self._index_data = {}

        key = self.compute_key(row)  # index key combination
        bucket = self._index_data.get(key, [])  # bucket list containing the rids/rid that have/has the same index_columns
        if self._kind != "INDEX":  # unique index type
            if len(bucket) > 0:
                raise KeyError("duplicate key for unique index type")
        bucket.append(rid)
        self._index_data[key] = bucket



    def compute_index_value(self, row):
        pass

    def delete_from_index(self, row, rid):
        pass

    def remove_from_index(self, row, rid):
        pass

    def _build(self):
        pass

    def __str__(self):
        s = "" + "\n"
        s = s + "index_name = " + self._index_name + "\n"
        s = s + "index_columns = "+ str(self._index_columns) + "\n"
        s = s + "kind = " + self._kind + "\n"
        s = s + "index_data = " + str(self._index_data) + "\n"

        return s

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
        :param column_names: list, name of the columns
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

            # build index on PRIMARY KEYS
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

        if self._indexes is None:
            self._indexes = {}

        # check the index names which should have no duplicates
        if index_name in self._indexes.keys():
            raise ValueError("index name already exists, please use a new index name")

        self._indexes[index_name] = Index(index_name=index_name, index_columns=column_list, kind=kind)

        # build index using all existing data
        self.build(index_name)

    def build(self, idx_name):
        """
        When adding a new index, we need to build this index using all the existing data
        and add them to the index data
        :param idx_name: index name
        :return:
        """
        idx = self._indexes[idx_name]
        for k, v in self._rows.items():
            idx.add_to_index(v, k)

    def drop_index(self, index_name):
        """
        drop index
        :param index_name:
        :return:
        """
        pass

    # def __str__(self):
    #     return None

    def _get_primary_key(self, r):
        pass

    def _get_primary_key_string(self, r):
        pass

    def _get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id

    def _add_row(self, r):
        pass

    def _remove_row(self, rid):
        pass

    def import_data(self, rows):
        """
        import data
        :param import_data:
        :return:
        """
        for r in rows:
            self.insert(r)


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

        if self._rows is None:
            self._rows = {}

        rid = self._get_next_row_id()

        # insert into every index in indexes
        if self._indexes is not None:
            for k,v in self._indexes.items():
                v.add_to_index(row, rid)

        self._rows[rid] = copy.copy(row)

    def _get_sub_template(self, tmp, table_name):
        pass

    def load_from_rows(self, table_name, rows):
        pass




