query_list = []


def get_initial_queries_compare(schema, table1, table2):
    """
    Initial compare method - compares the counts between two tables
    :param schema: schema from your athena db
    :param table1: first table for comparison
    :param table2: second table for comparison
    :return: list of queries to be executed
    """
    queries = [(f"SELECT COUNT(*) FROM {schema}.{table1}", f"SELECT COUNT(*) FROM {schema}.{table2}")]
    return queries


def compare_distinct_counts(schema, table1, table2, columns):
    """
    Distinct count comparison - each given column is compared between tables
    :param schema: schema from your athena db
    :param table1: first table for comparison
    :param table2: second table for comparison
    :param columns: list of columns to be compared
    :return: list of queries to be executed
    """
    queries = {}
    for column in columns:
        queries[column] = ((f"SELECT COUNT(DISTINCT {column}) FROM {schema}.{table1}",
                            f"SELECT COUNT(DISTINCT {column}) FROM {schema}.{table2}"))
    return queries


def get_initial_queries_both(schema, table1, table2, join, columns):
    """
    Comparison of not matched column values for a given join between two tables
    :param schema: schema from your athena db
    :param table1: first table for comparison
    :param table2: second table for comparison
    :param join: key column for joins between tables
    :param columns: list of columns to be compared
    :return: list of queries to be executed
    """
    queries = []
    for column in columns:
        queries.append((f"""SELECT COUNT(*) 
FROM {schema}.{table1} as t1
JOIN {schema}.{table2} as t2
ON(t1.{join} = t2.{join} AND t1.{column} <> t2.{column})"""))
    return queries
