import psycopg2
import os
import math
import multiprocessing as mp
from configparser import ConfigParser

def drop_tables(conn):
    cur = conn.cursor()
    cur.execute( 'DROP TABLE IF EXISTS igra_data' )
    cur.execute( 'DROP TABLE IF EXISTS igra_data_p' )
    cur.execute( 'DROP TABLE IF EXISTS igra_header' )
    cur.close()
    conn.commit()

def create_igra_header_table(conn):
    cur = conn.cursor()
    cur.execute( 'CREATE TABLE IF NOT EXISTS igra_header ('
                 'ID SERIAL PRIMARY KEY,'
                 'STATIONID VARCHAR(11),'
                 'YEAR INT,'
                 'MONTH INT,'
                 'DAY INT,'
                 'HOUR INT,'
                 'RELTIME SMALLINT,'
                 'P_SRC VARCHAR(8),'
                 'NP_SRC VARCHAR(8),'
                 'LAT FLOAT,'
                 'LON FLOAT'
                 ');'
                 )
    cur.close()
    conn.commit()


def create_igra_data_table(conn):
    cur = conn.cursor()
    cur.execute( 'CREATE TABLE IF NOT EXISTS igra_data ('
                 'ID SERIAL PRIMARY KEY,'
                 'HEADERID INT REFERENCES igra_header(ID),'
                 'LVLTYP1 SMALLINT,'
                 'LVLTYP2 SMALLINT,'
                 'ETIME INT,'
                 'PRESS INT,'
                 'PFLAG CHAR(1),'
                 'GPH INT,'
                 'ZFLAG CHAR(1),'
                 'TEMP SMALLINT,'
                 'TFLAG CHAR(1),'
                 'RH INT,'
                 'DPDP INT,'
                 'WDIR INT,'
                 'WSPD INT'
                 ');'
                 )
    cur.execute('CREATE INDEX IF NOT EXISTS IDX_IGRA_DATA_GPH ON igra_data(GPH);')
    cur.close()
    conn.commit()


def create_igra_data_partition_table(conn):
    print('Executing migration query')
    cur = conn.cursor()
    cur.execute( 'DROP TABLE IF EXISTS igra_data_p' )
    cur.execute('CREATE TABLE IF NOT EXISTS igra_data_p ('
             'ID SERIAL,'
             'HEADERID INT REFERENCES igra_header(ID),'
             'LVLTYP1 SMALLINT,'
             'LVLTYP2 SMALLINT,'
             'ETIME INT,'
             'PRESS INT,'
             'PFLAG CHAR(1),'
             'GPH INT,'
             'ZFLAG CHAR(1),'
             'TEMP SMALLINT,'
             'TFLAG CHAR(1),'
             'RH INT,'
             'DPDP INT,'
             'WDIR INT,'
             'WSPD INT'
             ') PARTITION BY RANGE (GPH);')
    cur.close()
    conn.commit()


def list_files(path):
    return [f for f in os.listdir( path ) if f.endswith( ".txt" )]


def import_data_from_file(filename):
    postgresql_config = config( 'config.ini', 'postgresql' )
    conn = psycopg2.connect( **postgresql_config )
    with open( 'data/{}'.format(filename), 'r' ) as f:
        for row in f:
            if row[0].startswith( '#' ):
                station_id = row[1:12]
                year = row[13:17]
                month = row[18:20]
                day = row[21:23]
                hour = None if row[24:26] == 99 else row[24:26]
                reltime = row[27:31]
                p_src = row[37:45]
                np_src = row[46:54].strip()
                lat = row[55:62]
                lon = row[63:71]
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO igra_header(STATIONID, YEAR, MONTH, DAY, HOUR, RELTIME, P_SRC, NP_SRC, LAT, LON) "
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING ID;",
                    (station_id, year, month, day, hour, reltime, p_src, np_src, lat, lon)
                )
                res = cur.fetchone()
                cur.close()
                conn.commit()
                header_id = res[0]
            else:
                lvltyp1 = row[0:1]
                lvltyp2 = row[1:2]
                etime = row[3:8]
                press = row[9:15]
                pflag = row[15:16]
                gph = row[16:21]
                zflag = row[21:22]
                temp = row[22:27]
                tflag = row[27:28]
                rh = row[29:33]
                dpdp = row[34:39]
                wdir = row[40:45]
                wspd = row[46:51]
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO igra_data("
                    "HEADERID, LVLTYP1, LVLTYP2, ETIME, PRESS, PFLAG, "
                    "GPH, ZFLAG, TEMP, TFLAG, RH, DPDP, WDIR, WSPD) " \
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                    (header_id, lvltyp1, lvltyp2, etime, press, pflag,
                     gph, zflag, temp, tflag, rh, dpdp, wdir, wspd )
                )
                cur.close()
                conn.commit()
    conn.close()


def partitions(conn, max, min=0, divide=1000):
    cur = conn.cursor()
    partition_min = math.floor( min / divide )
    partition_max = math.floor( max / divide ) + 1
    for i in range( partition_min, partition_max, 1 ):
        min_range = i * divide + 1 if i > 0 else 0
        max_range = i * divide + divide + 1 if i > 0 else i + divide + 1
        cur.execute('DROP TABLE IF EXISTS igra_data_p_{}'.format(i))
        sql = 'create table igra_data_p_{} partition of igra_data_p for values from ({}) to ({});'.format( i, min_range,
                                                                                                         max_range )
        cur.execute( sql )
    cur.execute( 'DROP TABLE IF EXISTS igra_data_p_default')
    cur.execute( 'create table igra_data_p_default partition of igra_data_p DEFAULT;' )
    cur.close()
    conn.commit()


def get_max(conn):
    cur = conn.cursor()
    cur.execute( 'SELECT max(GPH) FROM igra_data' )
    max_array = cur.fetchone()
    cur.close()
    conn.commit()
    return max_array[0]


def export_data_to_file(conn, min, max, divide=1000):
    partition_max = math.floor( max / divide ) + 1
    cur = conn.cursor()
    for i in range( min, partition_max, 1 ):
        sql = "COPY (SELECT ih.*, idp.* FROM igra_data_p_{} AS idp INNER JOIN igra_header AS ih on idp.headerid=ih.id) TO STDOUT WITH CSV DELIMITER ',' HEADER".format( i )
        with open( "exports/table_partition_{}.csv".format( i ), "w" ) as file:
            cur.copy_expert( sql, file )
    sql = "COPY (SELECT ih.*, idp.* FROM IGRA_DATA_P_DEFAULT AS idp INNER JOIN igra_header AS ih on idp.headerid=ih.id) TO STDOUT WITH CSV DELIMITER ',' HEADER"
    with open( "exports/table_partition_DEFAULT.csv", "w" ) as file:
        cur.copy_expert( sql, file )


def move_data(conn):
    cur = conn.cursor()
    cur.execute('INSERT INTO igra_data_p SELECT * FROM igra_data')
    cur.close()
    conn.commit()

def multiprocess():
    pool = mp.Pool( mp.cpu_count() )
    pool.map( import_data_from_file, list_files( 'data' ) )

def config(filename, section):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    section_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            section_config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return section_config


postgresql_config = config('config.ini', 'postgresql')
conn = psycopg2.connect( **postgresql_config )

drop_tables(conn)
create_igra_header_table( conn )
create_igra_data_table( conn )
create_igra_data_partition_table(conn)
multiprocess()
max = get_max( conn )
partitions( conn, max )
move_data(conn)
export_data_to_file( conn, 0, max )
conn.close()



