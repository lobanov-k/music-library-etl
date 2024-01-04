import configparser
import psycopg2
from prettytable import PrettyTable

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("10 top chart songs:")
    topSongsTable = PrettyTable(["Song ID", "Title", "Artist", "Plays count"])
    cur.execute('''
        SELECT sp.song_id, s.title, a.name, COUNT(*) as count
        FROM songplay sp
        LEFT JOIN song s ON sp.song_id = s.song_id
        LEFT JOIN artist a ON sp.artist_id = a.artist_id
        GROUP BY sp.song_id, s.title, a.name
        ORDER BY count DESC
        LIMIT 10
    ''')
    row = cur.fetchone()
    while row:
      topSongsTable.add_row([row[0], row[1], row[2], row[3]])
      row = cur.fetchone()
    print(topSongsTable)


    print("Plays of songs by hours:")
    hoursTable = PrettyTable(["Hour", "Plays count"])
    cur.execute('''
        SELECT t.hour, COUNT(sp.song_id) as count
        FROM songplay sp
        JOIN time t ON sp.start_time = t.start_time
        GROUP BY t.hour
        ORDER BY count
    ''')
    row = cur.fetchone()
    while row:
      hoursTable.add_row([row[0], row[1]])
      row = cur.fetchone()
    print(hoursTable)

if __name__ == "__main__":
    main()