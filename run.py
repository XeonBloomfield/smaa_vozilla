import os
import config
import json
import datetime
from car_status import CarStatus
import logging as log
import connection
import psycopg2

log.basicConfig(level=log.WARN)


def read_file_as_dict(file_path):
    # log.warn('Reading {file_path}'.format(file_path=file_path))
    with open(file_path, 'r') as f:
        return json.load(f)


def convert_name_to_timestamp(name):
    return datetime.datetime.strptime(name.split('.')[0], '%Y%m%d%H%M')


def process_data(dct, name):
    car_obj = None
    for car_data in dct['objects']:
        car_obj = CarStatus(car_data, convert_name_to_timestamp(name), cur)
        car_obj.process()
    log.info('Processed data from file = {}'.format(name))
    return car_obj


def read_file_into_dict(files_dictionary, entry_key, filepath):
    files_dictionary[entry_key] = read_file_as_dict(filepath)
    log.info('Processed file = {}'.format(entry_key))


# Walk into directories in filesystem
# Ripped from os module and slightly modified
# for alphabetical sorting
#
def sortedWalk(top, topdown=True, onerror=None):
    from os.path import join, isdir, islink

    names = os.listdir(top)
    names.sort()
    dirs, nondirs = [], []

    for name in names:
        if isdir(os.path.join(top, name)):
            dirs.append(name)
        else:
            nondirs.append(name)

    if topdown:
        yield top, dirs, nondirs
    for name in dirs:
        path = join(top, name)
        if not os.path.islink(path):
            for x in sortedWalk(path, topdown, onerror):
                yield x
    if not topdown:
        yield top, dirs, nondirs


if __name__ == '__main__':
    conn, cur = connection.connect()

    try:
        loaded_files = dict()

        old_root = None
        for root, dirs, files in sortedWalk(config.VOZILLA_JSON_DIR, topdown=False):
            if old_root is None:
                old_root = root

            # load json files into memory
            for name in files:
                path_to_file = os.path.join(root, name)
                read_file_into_dict(loaded_files, name, path_to_file)

            # check if we finished processing single directory
            if old_root != root:
                # process loaded files and delete them from memory
                car_status_obj = None
                for name in sorted(loaded_files.keys()):
                    car_status_obj = process_data(loaded_files[name], name)
                loaded_files = dict()

                # save processed files into database
                if car_status_obj is not None:
                    car_status_obj.db_operations()
                conn.commit()

                log.warning('Processed directory = {}'.format(old_root))
            old_root = root
    except (Exception, psycopg2.DatabaseError) as error:
        log.error(error)
    finally:
        connection.close_connection(conn, cur)
