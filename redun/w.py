import os
import subprocess
from redun import task
from redun.functools import map_, apply_func

ROOT = '/opt/node/collections/testdata/data'
DB_NAME = 'redun'
redun_namespace = 'walk'
PG_URI = "postgresql://localhost:5432/"


def create_db(PG_URI, DB_NAME):
    """create db if not exists."""
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    # Wait for database to accept connections.
    conn = psycopg2.connect(PG_URI)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
    cur = conn.cursor()
    sql = f"""
    CREATE DATABASE {DB_NAME};
    """
    try:
        cur.execute(sql)
    except psycopg2.errors.DuplicateDatabase:
        print('db already exists: ', DB_NAME)
        pass
    finally:
        conn.close()


def main():
    from redun import Scheduler
    from redun.config import Config

    # enable for PG
    # create_db(PG_URI, DB_NAME)
    scheduler = Scheduler(config=Config({
        # if commented out, memory-only setup, somewhat multi-threaded ~15s
        # "backend": {
        #     # "db_uri": "sqlite:///redun.db",  # > 90s sqlite single-threaded
        #     "db_uri": PG_URI + DB_NAME,  # 45s pg (also somehow single threaded)
        #     "automigrate": True,
        # },
        "executors.default": {
            "type": "local",
            "mode": "processes",
            "max_workers": 20,
            "scratch": "./tmp",
        }},
    ))
    scheduler.load()  # Auto-creates the redun.db file as needed and starts a db connection.
    result = scheduler.run(root())
    result['distinct_files'] = len(result['hashes'])
    del result['hashes']
    print('final result', result)



@task()
def root():
    print('+root')
    return walk(encode(ROOT))


def encode(str_):
    return str_.encode('utf-8', 'surrogateescape')


def decode(bytes_):
    return bytes_.decode('utf-8', 'surrogateescape')


@task()
def walk(path):
    # print(os.getpid(), '+walk', path)
    files = []
    dirs = []
    for item in os.listdir(path):
        item = os.path.join(path, item)
        if os.path.isdir(item):
            dirs.append(item)
        elif os.path.isfile(item):
            files.append(item)
        else:
            print('UNKNWON THING TYPE: ', item)
    files = map_(handle_file, files)
    dirs = map_(walk, dirs)
    return walk_combine(path, dirs, files)

@task()
def walk_combine(path, dirs, files):
    dir_cnt = 1 + sum(d['dir_cnt'] for d in dirs)
    file_cnt = len(files) + sum(d['file_cnt'] for d in dirs)
    file_paths = {}
    file_hashes = set(f['md5'] for f in files)
    for d in dirs:
        file_hashes |= d['hashes']

    return {
        'path': path,
        'file_cnt': file_cnt,
        'dir_cnt': dir_cnt,
        'hashes': file_hashes,
    }


@task()
def handle_file(path):
    # print(os.getpid(), '+handle_file', path)
    md5 = subprocess.check_output(f"md5sum '{decode(path)}'", shell=True)
    md5 = md5.split(b' ')[0].decode('ascii')
    return {'md5': md5, 'path': path, 'doc': handle_doc(md5)}


@task()
def handle_doc(md5):
    # print(os.getpid(), '+handle_doc', md5)
    return md5


if __name__ == '__main__':
    main()
