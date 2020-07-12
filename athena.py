from pyathena import connect
import pandas as pd
import queries as q
import secrets

#TODO convert to class
key, secret = secrets.pass_secrets()

region = 'region'
def get_connection(key, secret, s3_staging, region=region):
    """
    Gets connection to the athena
    :param key: aws access key
    :param secret: aws secret
    :param s3_staging: staging directory for Athena
    :param region: region of your Athena
    :return: connection object
    """
    return connect(aws_access_key_id=key,
                   aws_secret_access_key=secret,
                   s3_staging_dir=s3_staging,
                   region_name=region)


def get_columns(schema, table):
    """
    Returns columns for a given table
    :param schema: schema from your athena db
    :param table: table to get columns from
    :return: list of columns
    """
    cursor = conn.cursor()
    cursor.execute(f"""
                    SHOW COLUMNS IN {schema}.{table}""")
    return [x[0].strip() for x in cursor.fetchall()]


def compare_columns(tables):
    """
    Compare columns between two tables
    :param tables: list of tables to compare
    :return: boolean
    """
    #TODO implement stack and compare order as well
    cols = []
    for table in tables:
        cols.append(get_columns(table))
    if len(cols[0]) != len(cols[1]):
        print("Different number of columns")
        return False
    if set(cols[0]) != set(cols[1]):
        print("Different columns found")
        return False
    print("Test passed")
    return True


def compare_selects(query_tuple):
    """
    compare list of given selects
    :param query_tuple: Two selects for both tables to compare
    :return: #TODO
    """
    cursor = conn.cursor()
    q1, q2 = query_tuple
    res1 = cursor.execute(q1).fetchall()[0][0]
    res2 = cursor.execute(q2).fetchall()[0][0]
    if res1 == res2:
        print("PASS")
    else:
        print("ERROR!")
        print(res1, res2)


def get_differences(query):
    """
    Run comparison for query
    :param query: query as string
    :return: #TODO
    """
    try:
        cursor = conn.cursor()
        res = cursor.execute(query).fetchone()[0]
        if res>0:
            print("============")
            print("Issue found!")
            print("Please check the query: ")
            print(query)
            print(f"Number of differences: {res}")
    except Exception as e:
        print(e)



def run_tests():
    """
    #TODO convert to main method
    :return:
    """
    tables = ('test_table1', 'test_table2')
    columns = get_columns(tables[0])

    print("Test 1. compare columns")
    compare_columns(tables)
    print("Test 2. compare distinct counts")
    queries_test_2 = q.compare_distinct_counts(*tables, columns)
    for column, query_tuple in queries_test_2.items():
        print(column)
        compare_selects(query_tuple)
    print("Test 3. initial join")
    queries_test_3 = q.get_initial_queries_compare(*tables)
    for query_tuple in queries_test_3:
        compare_selects(query_tuple)
    queries_2 = q.get_initial_queries_both(*tables, 'keycolumn', columns)
    for query in queries_2:
        get_differences(query)


#TODO convert to class
conn = get_connection(key, secret, 's3 address')
run_tests()
