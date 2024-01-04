import configparser
import psycopg2
from prettytable import PrettyTable
from sql_queries import copy_table_queries_map, insert_table_queries_map, drop_final_table_queries
from helpers import check_table_rows_count

def load_staging_tables(cur, conn):
    for record in copy_table_queries_map:
        cur.execute(record[1])
        conn.commit()

        # print tables length
        cur.execute(("""
            SELECT COUNT(*) from {}
        """).format(record[0]))
        row = cur.fetchone()
        print("Table '%s' length: %s" % (record[0], row[0]))



def insert_tables(cur, conn):
    for record in insert_table_queries_map:
        cur.execute(record[1])
        conn.commit()
        
        # print tables length
        cur.execute(("""
            SELECT COUNT(*) from {}
        """).format(record[0]))
        row = cur.fetchone()
        print("Table '%s' length: %s" % (record[0], row[0]))

def drop_tables(cur, conn):
    for query in drop_final_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # s = cur.execute('SELECT * FROM stl_load_errors')

    # s = cur.execute('''select le.starttime, d.query, d.line_number, d.colname, d.value,
    #     le.raw_line, le.err_reason    
    #     from stl_loaderror_detail d, stl_load_errors le
    #     where d.query = le.query
    #     order by le.starttime desc
    #     limit 100'''
    # )
    # row = cur.fetchone()
    # while row:
    #     print(row)
    #     row = cur.fetchone()
    # drop_tables(cur, conn)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()