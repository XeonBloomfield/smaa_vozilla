import psycopg2
import config
import logging as log
import datetime


def connect():
    global conn
    conn = psycopg2.connect(host=config.HOST,
                                port=config.PORT,
                                dbname=config.DATABASE,
                                user=config.DB_USER,
                                password=config.DB_PASSWORD)
    cur = conn.cursor()
    log.info('Connection to {db} on {host} was established.'.format(
            db=config.DATABASE, host=config.HOST))
    return conn, cur


def close_connection(conn, cur):
    cur.close()
    if conn is not None:
        conn.close()
        log.info('Connection successfully closed.')


def convert_value(vals):
    if type(vals) == str:
        return '\'{}\''.format(vals)
    elif type(vals) == datetime.datetime:
        return '\'{}\''.format(str(vals))
    elif type(vals) == int or type(vals) == float or type(vals) == bool:
        return str(vals)
    elif vals is None:
        return 'null'
    return vals
    
def select(cur, query):
    log.info('Running: ' + query)
    cur.execute(query)
    return cur.fetchall()


def select_vehicle(cur, vehicle_id):
    try:
        query = 'SELECT 1 FROM "Vehicle" WHERE id=\'{}\''.format(vehicle_id)
        cur.execute(query)
        query_result = cur.fetchone()
        if query_result is None:
            return False
        else:
            return True
    except (Exception, psycopg2.DatabaseError) as error:
        log.error(error)


def insert(cur, table_name, values):
    try:
        # Build query
        values_str = ""
        if len(values) == 0:
            log.error('0 values to input to table ' + table_name)
            return
        for val in values:
            if len(values_str) > 0:
                values_str += ','
            single_value_str = ','.join(list(map(lambda x: convert_value(x), val)))
            values_str += '({})'.format(single_value_str)
        log.info('Inserting {} records.'.format(len(values)))

        query = 'INSERT INTO "{table}" VALUES {values};'.format(
            table=table_name, values=values_str)
        log.info('Running: ' + query)

        # Execute
        cur.execute(query)
        # conn.commit()  # <- We MUST commit to reflect the inserted data
        log.info('Successfully inserted {num_of_records} records to {table}.'.format(
            num_of_records=len(values), table=table_name))

    except (Exception, psycopg2.DatabaseError) as error:
        log.error(error)
