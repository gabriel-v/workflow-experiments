import json
from functools import wraps
import os
import subprocess

# import redun.file
# from redun.file import LocalFileSystem
# redun.file.get_filesystem = lambda *k, **w: LocalFileSystem()

from redun import task
from redun.functools import map_, apply_func, force


from qbase import DB_NAME, create_queue_table, create_db, PG_URI


ROOT = '/opt/node/collections/testdata/data'
redun_namespace = 'walk'

DIR_BATCH_COUNT = 500
FILE_BATCH_SIZE_BYTES = 5 * 2**20 # 50MB batches


def main():
    from redun import Scheduler
    from redun.config import Config

    # enable for PG
    create_db()
    scheduler = Scheduler(config=Config({
        # if commented out, memory-only setup, somewhat multi-threaded ~15s
        "backend": {
            # "db_uri": "sqlite:///redun.db",  # > 90s sqlite single-threaded
            "db_uri": PG_URI + DB_NAME,  # 45s pg (also somehow single threaded)
            "automigrate": True,
        },
        "executors.default": {
            "type": "local",
            "mode": "processes",
            "max_workers": 40,
            "scratch": "./tmp",
        }},
    ))
    scheduler.load()  # Auto-creates the redun.db file as needed and starts a db connection.
    result = scheduler.run(root())
    # result['distinct_files'] = len(result['hashes'])
    # del result['hashes']
    print('final result', result)



@task()
def root():
    print('+root')
    return walk([encode(ROOT)])


def encode(str_):
    return str_.encode('utf-8', 'surrogateescape')


def decode(bytes_):
    return bytes_.decode('utf-8', 'surrogateescape')


@task()
def walk(paths):
    files = []
    file_size = 0
    dirs = []
    dirs_fut = []
    files_fut = []
    for path in paths:
        # print(os.getpid(), '+walk', path)
        for item in sorted(os.listdir(path)):
            item = os.path.join(path, item)
            if os.path.isdir(item):
                dirs.append(item)
                if len(dirs) > DIR_BATCH_COUNT:
                    dirs_fut.append(walk(dirs))
                    dirs = []
            elif os.path.isfile(item):
                files.append(item)
                file_size += os.stat(item).st_size
                if file_size > FILE_BATCH_SIZE_BYTES:
                    files_fut.append(handle_files(files))
                    files = []
                    file_size = 0
            else:
                print('UNKNWON THING TYPE: ', item)
    if files:
        files_fut.append(handle_files(files))
    if dirs:
        dirs_fut.append(walk(dirs))
    return walk_combine(paths, dirs_fut, files_fut)


@task()
def walk_combine(paths, dirs, files):
    dirs = sum(dirs, start=[])
    files = sum(files, start=[])
    dir_cnt = sum(d['dir_cnt'] for d in dirs) + len(paths)
    file_cnt = len(files) + sum(d['file_cnt'] for d in dirs)
    file_paths = {}
    # file_hashes = set(f['md5'] for f in files)
    # for d in dirs:
    #     file_hashes |= d['hashes']

    return [{
        'path': paths[0],
        'file_cnt': file_cnt,
        'dir_cnt': dir_cnt,
        # 'hashes': file_hashes,
    }]


@task()
def handle_files(paths):
    return [handle_file(x) for x in paths]

def handle_file(path):
    # print(os.getpid(), '+handle_file', path)
    md5 = subprocess.check_output(["md5sum", '{decode(path)}'], shell=True)
    md5 = md5.split(b' ')[0].decode('ascii')
    return {'md5': md5, 'path': path, 'doc': handle_doc(md5)}


# @task()
def handle_doc(md5):
    # print(os.getpid(), '+handle_doc', md5)
    return {"md5": md5}


if __name__ == '__main__':
    main()
