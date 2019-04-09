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
        result = {}
        result["name"] = self._index_name
        result["columns"] = self._index_columns
        result["kind"] = self._kind
        result["index_data"] = self._index_data

        return result

    def from_json(self):
        pass

    def matches_index(self, template):
        """
        Return how many distinct entries there are in an index data
        :param template:
        :return:
        """
        return self._index_distinct_value_count(template)

    def _index_distinct_value_count(self, template):
        """
        Return how many distinct entries there are in an index data
        :param template:
        :return:  int type or None. If the template keys contain all the index columns, return how many distinct
                  entries there are in an index data, otherwise, return None.
        """
        k = set(list(template.keys()))
        c = set(self._index_columns)

        if c.issubset(k):
            # index matches
            if self._index_data is not None:
                kk = len(self._index_data.keys())
            else:
                kk = 0
        else:
            kk = None

        return kk

    def find_rows(self, tmp):
        """
        Assuming the tmp contains all the index columns.
        Using the index to find the matching rows by template. Return the rows with key value that matches the template
        :param tmp: template
        :return: list type. Rows
        """
        t_vals = [tmp[k] for k in self._index_columns]
        t_s = "_".join(t_vals)

        # get the corresponding index bucket
        d = self._index_data.get(t_s, None)

        return d



class CSVDataTable:
    """
    The database engine behaviours implementation on CSV tables
    """
    _default_directory = "/home/zhida/Desktop/Database/code/HW3/DB/"

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

        d = {
            "state": {
                "table_name": self._table_name,
                "primary_key_columns": self._primary_key_columns,
                "next_row_id": self._get_next_row_id(),
                "column_names": self._column_names
            }
        }

        filename = CSVDataTable._default_directory + self._table_name + ".json"
        d["rows"] = self._rows

        for k, v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string
            d["indexes"] = idxs

        d = json.dumps(d, indent=2)
        with open(filename, "w+") as output:
            output.write(d)


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

    def get_best_index(self, tmp):
        """

        :param tmp: dict type. template
        :return: String type. most selective index name
        """

        best = None
        name = None

        if self._indexes is not None:
            for k, v in self._indexes.items():
                cnt = v.matches_index(tmp)
                if cnt is not None:
                    if best is None:
                        best = cnt
                        name = k
                    else:
                        if cnt > best:
                            best = cnt
                            name = k
        return name


    def find_by_index(self, tmp, idx: Index):
        r = idx.find_rows(tmp)
        res = [self._rows[rid] for rid in r]
        return res

    def find_by_scan_template(self, tmp, rows):
        pass

    def find_by_template(self, tmp, fields, use_index=True):
        """

        :param tmp: Dictionary type, template to match
        :param fields: Fields to get, like project clause
        :param use_index: if True, use index, if False, no index used
        :return: CSVDataTable type . A new derived CSVDataTable
        """
        idx = self.get_best_index(tmp)
        logging.debug("Index used is = %s", idx)

        if idx is None or use_index is False:
            # if not using indexes
            result = self.find_by_scan_template(tmp=tmp, rows=self.get_rows())
        else:
            # use index to speed up
            idx = self._indexes[idx]
            # get the rows find by the index
            res_index = self.find_by_index(tmp=tmp, idx=idx)
            # scan on the rows left
            result = self.find_by_scan_template(tmp, res_index)

        derived_table_name = "Derived CSVDataTable: " + self._table_name
        new_t = CSVDataTable(table_name=derived_table_name, loadit=True)
        new_t.load_from_rows(table_name=derived_table_name, rows=result)

        return new_t



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




