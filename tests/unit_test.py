from src import CSVDataTable
import logging
import csv
import time
import json

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
    r["first_name"] = "fei"
    r["uni"] = "xx"
    t.insert(r)
    if b_index:
        index_columns = ["first_name", "last_name"]
        t.add_index(index_name="Name", column_list=index_columns, kind="INDEX")

    return t

def make_test_table2(b_index=True):
    t = CSVDataTable.CSVDataTable(table_name="teams", column_names=["uni", "bat", "teamID"],
                                  primary_key_columns=["uni"], loadit=None)
    t.insert({
        "uni": "zz",
        "bat": 2,
        "teamID": "spurs"
    })
    t.insert({
        "uni": "yy",
        "bat": 3,
        "teamID": "spurs"
    })
    t.insert({
        "uni": "xx",
        "bat": 2,
        "teamID": "lakers"
    })

    if b_index:
        index_columns = ["teamID"]
        t.add_index(index_name="Team", column_list=index_columns, kind="INDEX")

    return t

def test_create_table():

    logging.debug("-------- test_create_table ---------")
    t = CSVDataTable.CSVDataTable(table_name="students", column_names=["uni", "first_name", "last_name"],
                                  primary_key_columns=["uni"], loadit=None)

    print(t)
    print("\n\n\n\n\n\n")

def test_load_data():

    logging.debug("-------- test_load_data ---------")
    rows, cols = load("/home/zhida/Desktop/Database/code/HW3/CSVFile/city.csv")
    t = CSVDataTable.CSVDataTable(table_name="city", column_names=cols,
                                  primary_key_columns=["id"], loadit=None)
    t.import_data(rows=rows)

    print("t = ", json.dumps(t._rows, indent=2))
    print("\n\n\n\n\n\n")

def test_compute_key_add_to_index():

    logging.debug("-------- test_compute_key_add_to_index ---------")
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
    print("\n\n\n\n\n\n")

def test_insert_on_index():

    logging.debug("-------- test_insert_on_index ---------")
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
    print("\n\n\n\n\n\n")

def test_save_db():

    logging.debug("-------- test_save_db ---------")
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
    print("save DB success!\n")
    print("\n\n\n\n\n\n")

def test_save_db2():

    logging.debug("-------- test_save_db ---------")
    rows, cols = load("/home/zhida/Desktop/Database/code/HW3/CSVFile/city.csv")
    t = CSVDataTable.CSVDataTable(table_name="city", column_names=cols,
                                  primary_key_columns=["id"],
                                  loadit=None)
    t.import_data(rows=rows)
    t.save()
    print("save DB success!\n")
    print("\n\n\n\n\n\n")

def test_save_db3():

    logging.debug("-------- test_save_db ---------")
    rows, cols = load("/home/zhida/Desktop/Database/code/HW3/CSVFile/Philippines_poverty_estimates.csv")
    t = CSVDataTable.CSVDataTable(table_name="Philippines_poverty_estimates", column_names=cols,
                                  primary_key_columns=["PCODE"],
                                  loadit=None)
    t.import_data(rows=rows)
    t.add_index("YEAR2012", ["Prelim_2012", "Pov_2012"], "INDEX")
    t.save()
    print("save DB success!\n")
    print("\n\n\n\n\n\n")

    time1 = time.time()
    for i in range(1000):
        t.find_by_template(tmp={"Municipality_City": "San Juan"}, use_index=False)
    time2 = time.time()
    print("1000 iterations takes time = ", str(time2- time1))

def test_save_db4():

    logging.debug("-------- test_save_db ---------")
    rows, cols = load("/home/zhida/Desktop/Database/code/HW3/CSVFile/beach-weather-stations-automated-sensors-1.csv")
    t = CSVDataTable.CSVDataTable(table_name="automated-sensors", column_names=cols,
                                  primary_key_columns=["Measurement ID"],
                                  loadit=None)
    t.import_data(rows=rows)
    t.add_index("Station Name", ["Station Name"], "INDEX")
    t.save()
    print("save DB success!\n")
    print("\n\n\n\n\n\n")

    time1 = time.time()
    for i in range(100):
        t.find_by_template(tmp={"Station Name": "Oak Street Weather Station",
                                "Wet Bulb Temperature": 55}, use_index=False)
    time2 = time.time()
    print("takes time = ", str(time2 - time1) + " second")

    time1 = time.time()
    for i in range(100):
        t.find_by_template(tmp={"Station Name": "Oak Street Weather Station",
                                "Wet Bulb Temperature": 55}, use_index=True)
    time2 = time.time()
    print("takes time = ", str(time2 - time1) + " second")



def test_load_db():

    logging.debug("-------- test_load_db ---------")
    t = make_test_table(b_index=False)
    print("\nbefore load()")
    print(t)
    t.load()
    print("\n\n\nafter load()")
    print(t)
    print("\n\n\n\n\n\n")


def test_find_by_template():

    logging.debug("-------- test_find_by_template ---------")
    t = make_test_table(b_index=False)

    tmp = {
        "first_name": "zhida"
    }
    fields = ["uni", "last_name"]
    new_t = t.find_by_template(tmp, fields, use_index=False)
    print("new_t = ", new_t)
    print("\n\n\n\n\n\n")

def test_insert():

    logging.debug("-------- test_insert ---------")
    t = make_test_table()
    r = {
        "uni": "dd",
        "first_name": "vince",
        "last_name": "zhang"
    }
    print("\nbefore insert")
    print(t)
    t.insert(r)
    print("\nafter insert")
    print(t)
    print("\n\n\n\n\n\n")


def test_delete():

    logging.debug("-------- test_delete ---------")
    t = make_test_table()
    tmp = {
        "first_name": "zhida"
    }
    print("\nbefore delete")
    print(t)
    t.delete(tmp)
    print("\nafter delete")
    print(t)
    print("\n\n\n\n\n\n")

def test_import_data():

    logging.debug("-------- test_import_data ---------")
    t = make_test_table()
    rows = [
        {
            "uni": "cc",
            "first_name": "xin",
            "last_name": "wei"
        },
        {
            "uni": "ff",
            "first_name": "li",
            "last_name": "zhang"
        }
    ]
    t.import_data(rows)
    t.save()
    print(t)
    print("\n\n\n\n\n\n")


def test_access_path():

    logging.debug("-------- test_access_path ---------")
    t = make_test_table(b_index=True)
    on_fields = ["first_name", "last_name"]
    path, count = t._get_access_path(on_fields)

    print("path = ", path, ",count = ", count)
    print("\n\n\n\n\n\n")


def test_join():

    logging.debug("-------- CSVDataTable.join() ---------")
    t1 = make_test_table()
    t2 = make_test_table2()

    new_t = t1.join(t2, on_fields=["uni"], where_template={"teams.teamID": "spurs"})

    print(new_t)
    print("rows =", json.dumps(new_t.get_rows(), indent=2))
    print("\n\n\n\n\n\n")






test_create_table()
test_load_data()
test_compute_key_add_to_index()
test_insert_on_index()
test_save_db()
test_save_db2()
# test_save_db3()
# test_save_db4()
test_find_by_template()
test_load_db()
test_insert()
test_delete()
test_import_data()
test_access_path()
test_join()