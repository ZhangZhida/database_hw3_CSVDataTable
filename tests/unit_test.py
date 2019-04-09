from src import CSVDataTable
import logging
import csv

logging.basicConfig(level=logging.DEBUG)


def load(filename):
    """

    :param filename:
    :return:
        result: list, all the rows
        cols:   list, column names
    """

    result = []
    cols = None
    with open(filename, 'r') as file:
        rdr = csv.DictReader(file)
        cols = rdr.fieldnames
        for r in rdr:
            result.append(r)

    return result, cols

def make_test_table(b_index=True):
    t = CSVDataTable.CSVDataTable(table_name="students", column_names=["uni", "first_name", "last_name"],
                                  primary_key_columns=["uni"], loadit=None)
    r = {
        "uni": "zz",
        "first_name": "zhida",
        "last_name": "zhang"
    }
    t.insert(r)
    r["uni"] = "yy"
    t.insert(r)
    if b_index:
        index_columns = ["first_name", "last_name"]
        t.add_index(index_name="name", column_list=index_columns, kind="INDEX")

    return t

def test_create_table():
    t = CSVDataTable.CSVDataTable(table_name="students", column_names=["uni", "first_name", "last_name"],
                                  primary_key_columns=["uni"], loadit=None)

    print("t = ", t)

def test_load_data():
    rows, cols = load("/home/zhida/Desktop/Database/code/HW3/data/city.csv")
    t = CSVDataTable.CSVDataTable(table_name="city", column_names=cols,
                                  primary_key_columns=["id"], loadit=None)
    t.import_data(rows=rows)

    print("t = ", t._rows)

def test_compute_key_add_to_index():
    i = CSVDataTable.Index(index_name="test", index_columns=["last_name", "first_name"], kind="INDEX")
    r = {
        "last_name": "zhang",
        "first_name": "zhida",
        "uni": "zz2578"
    }
    key = i.compute_key(r)
    print("key = ", key)

    i.add_to_index(r, "2")
    i.add_to_index(r, "3")
    print("i = ", i)

def test_insert_on_index():
    t = CSVDataTable.CSVDataTable(table_name="students", column_names=["uni", "first_name", "last_name"],
                                  primary_key_columns=["uni"], loadit=None)
    r = {
        "uni": "zz",
        "first_name": "zhida",
        "last_name": "zhang"
    }
    t.insert(r)
    r["uni"] = "yy"
    t.insert(r)

    index_columns = ["first_name", "last_name"]
    t.add_index(index_name="name", column_list=index_columns, kind="INDEX")
    print("t = ", t)

def test_save_db():
    t = CSVDataTable.CSVDataTable(table_name="students", column_names=["uni", "first_name", "last_name"],
                                  primary_key_columns=["uni"], loadit=None)
    r = {
        "uni": "zz",
        "first_name": "zhida",
        "last_name": "zhang"
    }
    t.insert(r)
    r["uni"] = "yy"
    t.insert(r)

    index_columns = ["first_name", "last_name"]
    t.add_index(index_name="name", column_list=index_columns, kind="INDEX")

    # save
    t.save()

def test_find_by_template():
    t = make_test_table()

    tmp = {
        "last_name": "zhang"
    }
    fields = tmp.keys()
    new_t = t.find_by_template(tmp, fields, use_index=True)
    print("new_t = ", new_t)










# test_create_table()
# test_load_data()
# test_compute_key_add_to_index()
# test_insert_on_index()
# test_save_db()
test_find_by_template()