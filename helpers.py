from prettytable import PrettyTable

def check_table_rows_count(cur, conn, table):
  cur.execute(("""
    SELECT COUNT(*) from {}
  """).format(table))
  table = PrettyTable(["Table", "Length"])
  row = cur.fetchone()
  table.add_row([table, row[0]])
  print(table)