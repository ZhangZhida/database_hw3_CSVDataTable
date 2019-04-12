import json
import copy
import logging




class Index:

    def __init__(self, index_name=None, index_columns=None, kind=None, loadit=None):
        """

        :param index_name:
        :param index_columns:
        :param kind:
        :param loadit: dict type. If provided, will load all the data from this external index.
        """
        if loadit is None:
            self._index_name = index_name
            self._index_columns = index_columns
            self._kind = kind

            self._index_data = None  # dictionary that contains the index data
        else:
            idx = self.from_json(loadit)
            # self._index_name = idx._index_name
            # self._index_columns = idx._index_columns
            # self._kind = idx._kind
            # self._index_data = idx._index_data




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

    def from_json(self, dict):
        self._index_name = dict["name"]
        self._index_columns = dict["columns"]
        self._kind = dict["kind"]
        self._index_data = dict["index_data"]


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
    show_index_data = True

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
        Add index with no index data.
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

    def _get_primary_key(self, r: dict):
        """

        :param r: dict type.
        :return: dict type. Primary key value.
        """
        res = {}
        for k in self._primary_key_columns:
            res[k] = r[k]
        return res


    def _get_primary_key_string(self, r):
        key = self._get_primary_key(r)
        key_str = "_".join(key.values())
        return key_str

    def _get_index_key(self, r, idx):
        """
        get the value of index key in a row
        :param r:
        :param idx:
        :return: dict type.
        """
        res = {}
        for k in idx._index_columns:
            if k in r.keys():
                res[k] = r[k]
            else:
                return None
        return res

    def _get_index_key_string(self, r, idx):

        res = self._get_index_key(r, idx)
        if res is None:
            return None
        else:
            return "_".join(res.values())


    def _get_next_row_id(self):
        self._next_row_id += 1
        return self._next_row_id

    def _add_row(self, r):
        pass

    def _remove_row(self, rid):
        pass

    def import_data(self, rows: list):
        """
        import data
        :param rows:
        :return:
        """
        for r in rows:
            self.insert(r)

    def __str__(self):
        s = "\ntable name = " + self._table_name + "\n"
        s = s + "primary_key_columns = " + str(self._column_names) + "\n"
        s = s + "column_names = " + str(self._column_names) + "\n"
        s = s + "new_row_id = " + str(self._next_row_id) + "\n"

        if self.show_index_data:
            for k, v in self._indexes.items():
                s = s + str(v)
        return s



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

        filename = self._get_index_filename()
        d["rows"] = self._rows

        for k, v in self._indexes.items():
            idxs = d.get("indexes", {})
            idx_string = v.to_json()
            idxs[k] = idx_string
            d["indexes"] = idxs

        d = json.dumps(d, indent=2)
        with open(filename, "w+") as output:
            output.write(d)

    def _get_index_filename(self):

        return CSVDataTable._default_directory + self._table_name + ".json"

    def load(self):
        """
        load index data and state info from local .json file
        :return:
        """
        filename = self._get_index_filename()
        with open(filename, "r") as inputfile:
            d = json.load(inputfile)

            state = d["state"]
            self._table_name = state["table_name"]
            self._primary_key_columns = state["primary_key_columns"]
            self._column_names = state["column_names"]
            self._next_row_id = state["next_row_id"]
            self._rows = d["rows"]

            for k, v in d["indexes"].items():
                idx = Index(loadit=v)
                if self._indexes is None:
                    self._indexes = {}
                self._indexes[k] = idx


    def get_rows_with_rids(self):
        pass

    def get_rows(self):
        return self._rows

    def matches_template(self, row, tmp):
        pass

    def get_best_index(self, tmp, return_count=False):
        """

        :param tmp: dict type. template
        :return: (String type, int type). most selective index name and its best count
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
        if return_count:
            return name, best
        else:
            return name


    def find_by_index(self, tmp, idx: Index):
        r = idx.find_rows(tmp)
        # res = [self._rows[rid] for rid in r]
        res = {}
        for rid in r:
            res[rid] = self._rows[rid]
        return res


    def find_by_scan_template(self, tmp, rows: dict):
        """
        scan the table using tmp and without using index info.
        :param tmp:  dict type. template to match
        :param rows: list type. rows to scan
        :return: list type. Matched rows.
        """
        res = {}
        for k, v in rows.items():
            # check if match the template
            b_match = True
            for kk in tmp.keys():
                if tmp[kk] != v[kk]:
                    b_match = False
            if b_match:
                res[k] = v
        return res


    def find_by_template(self, tmp, fields=None, use_index=True, debug_log=True):
        """

        :param tmp: Dictionary type, template to match
        :param fields: list type. Fields to get, like project clause
        :param use_index: if True, use index, if False, no index used
        :return: CSVDataTable type . A new derived CSVDataTable
        """
        idx = self.get_best_index(tmp)
        if debug_log:
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

        # filter columns using fields that the user requests
        if fields:
            filtered_result = {}
            for k, v in result.items():
                for field in fields:
                    if k not in filtered_result.keys():
                        filtered_result[k] = {}
                    filtered_result[k][field] = result[k][field]
            result = filtered_result

        derived_table_name = "Derived CSVDataTable: " + self._table_name
        new_t = CSVDataTable(table_name=derived_table_name, primary_key_columns=self._primary_key_columns, loadit=True)

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

    def delete(self, tmp):
        """
        delete those rows matching the template
        :param tmp: template to match
        :return:
        """
        # find the rows matching the template
        idx = self.get_best_index(tmp)
        if idx is not None:
            idx = self._indexes[idx]
            # get the rows find by the index
            res_index = self.find_by_index(tmp=tmp, idx=idx)
            # scan on the rows left
            res = self.find_by_scan_template(tmp, res_index)
        else:
            res = self.find_by_scan_template(tmp, self.get_rows())

        # delete data in indexes
        for rid, row in res.items():
            for idx in self._indexes.values():
                key_str = self._get_index_key_string(row, idx)
                rid_list = idx._index_data[key_str]
                if rid in rid_list:
                    rid_list.remove(rid)
                if len(rid_list) == 0:
                    del idx._index_data[key_str]


        # delete data in rows
        for rid in res.keys():
            del self._rows[rid]


        print("no")

    def _get_access_path(self, on_fields):
        """

        :param on_fields:  on clause fields
        :return:  (string type, int type). return access path string and distinct count
        """
        fake_tmp = {}
        fake_value = 0
        for field in on_fields:
            fake_tmp[field] = fake_value
        path, count = self.get_best_index(fake_tmp, return_count=True)
        return path, count


    def _choose_scan_probe_table(self, right_tb, on_fields):
        left_path, left_count = self._get_access_path(on_fields)
        right_path, right_count = right_tb._get_access_path(on_fields)

        if left_path is None and right_path is None:
            return self, right_tb
        elif left_path is None and right_path is not None:
            return right_tb, self
        elif left_path is not None and right_path is None:
            return self, right_tb
        elif right_count < left_count:
            return self, right_tb
        else:
            return self, right_tb

    def _get_sub_where_template(self, where_template):
        """
        select the items in where template that related to the self table and form a new sub where clause
        :param where_template: dict type. like {"People.nameLast": "James", "People.nameFirst": "Lebron"}
        :return: dict type. return None if no field matches to make a sub template
        """
        sub_template = None
        if where_template is None:
            return None
        for k,v in where_template.items():
            tbl, column = k.split(".")
            if tbl == self._table_name:
                if sub_template is None:
                    sub_template = {}
                sub_template[column] = v
        return sub_template

    def _get_on_template(self, l_r, on_fields):
        on_template = None
        for field in on_fields:
            if on_template is None:
                on_template = {}
            on_template[field] = l_r[field]

        return on_template

    def _join_l_row_r_rows(self, left_r, right_rs, on_fields):
        """

        :param left_r: dict type. A single row
        :param right_rs:  dict type. rows to join.
        :param on_fields:
        :return:
        """
        new_rows = []
        for right_r in right_rs:
            new_rows.append(self._join_row_and_row(left_r, right_r, on_fields))
        return new_rows

    def _join_row_and_row(self, left_r, right_r, on_fields):
        res = {}
        for k, v in left_r.items():
            res[k] = v
        for k, v in right_r.items():
            if k not in on_fields:
                res[k] = v
        return res


    def join(self, right_tb, on_fields, where_template=None, project_fields=None):
        """
        Self is the left (scan) table. right_table is the probe table
        :param right_tb:
        :param on_fields:
        :param where_template:
        :param project_fields:
        :return:
        """
        # swap scan table and probe table based on the selectivity of those tables
        scan_tb, probe_tb = self._choose_scan_probe_table(right_tb, on_fields)

        scan_sub_template = scan_tb._get_sub_where_template(where_template)
        probe_sub_template = probe_tb._get_sub_where_template(where_template)

        if scan_sub_template is not None:
            scan_rows = self.find_by_template(scan_sub_template)
        else:
            scan_rows = scan_tb

        join_result = {}
        join_next_rid = 1

        # probing on the right table

        for rid, l_r in scan_rows._rows.items():
            on_template = self._get_on_template(l_r, on_fields)
            if probe_sub_template is not None:
                right_where = {**probe_sub_template, **on_template}
            else:
                right_where = on_template

            # find on the probe table
            current_right_rows = right_tb.find_by_template(right_where)

            if current_right_rows is not None and len(current_right_rows._rows) > 0:
                new_rows_list = self._join_l_row_r_rows(l_r, current_right_rows._rows.values(), on_fields)
                new_rows = {}
                for r in new_rows_list:
                    new_rows[join_next_rid] = r
                    join_next_rid += 1

                join_result.update(new_rows)

        # make new CSVDataTable from the join_result
        derived_table_name = "Derived CSVDataTable: " + self._table_name
        joined_column_names = list(set([*scan_tb._column_names, *probe_tb._column_names]))
        new_t = CSVDataTable(table_name=derived_table_name, primary_key_columns=self._primary_key_columns,
                             column_names=joined_column_names, loadit=True)

        new_t.load_from_rows(table_name=derived_table_name, rows=join_result)

        return new_t


    def _get_sub_template(self, tmp, table_name):
        pass

    def load_from_rows(self, table_name, rows):
        self._rows = rows
        if rows.keys() is not None and len(rows.keys()) > 0:
            self._next_row_id = max(rows.keys()) + 1
        if self._primary_key_columns:
            self.add_index("PRIMARY", self._primary_key_columns, "PRIMARY")





