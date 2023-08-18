import json
from functools import wraps
import os
import subprocess

import redun.file
from redun.file import LocalFileSystem
redun.file.get_filesystem = lambda *k, **w: LocalFileSystem()
from redun import File as RedunFile, Dir as RedunDir

import redun.scripting
redun.scripting.prepare_command = lambda x, **k: x
redun.scripting.get_command_eof = lambda x, **k: 'EOFFFFF999'

from redun import task, script
from redun.functools import map_, apply_func, force


class File(RedunFile):
    def __init__(self, path):
        # hack to avoid fixing broken paths
        self.filesystem = LocalFileSystem()
        self.path = path
        self.stream = None
        self._hash = None
        self.classes.File = File
    def __setstate__(self, state: dict) -> None:
        self.path = state["path"]
        self._hash = state["hash"]
        self.filesystem = LocalFileSystem()


# class Dir(RedunDir):
#     def __setstate__(self, state: dict) -> None:
#         self.pattern = os.path.join(state["path"], b"**")
#         self.path = state["path"]
#         self._hash = state["hash"]
#         self.filesystem = LocalFileSystem()
#         self._files = None
#     def __init__(self, path: str):
#         # path = path.rstrip("/")
#         self.path = path
#         pattern = os.path.join(path, b"**")
#         self.pattern = pattern
#         self.filesystem: FileSystem = LocalFileSystem()
#         self._hash: Optional[str] = None
#         self._files: Optional[List[File]] = None
#         self.classes.File = File


from qbase import DB_NAME, create_queue_table, PG_URI, init

import qexecutor


ROOT = b'/opt/node/collections/testdata/data'
redun_namespace = 'w'

DIR_BATCH_COUNT = 1000
FILE_BATCH_SIZE_BYTES = 66 * 2**20 # 10MB batches
FILE_BATCH_MAX_COUNT = 10000

os.makedirs('./.redun', exist_ok=True)
REDUN_CONFIG_FILE = './.redun/redun.ini'
REDUN_CONFIG_VAL = {
    "backend": {
        "db_uri": PG_URI + DB_NAME,
        "automigrate": True,
    },
    "executors.default": {
        "type": "pg",
    }
}


def write_config_file(config_dict, path):
    import configparser
    with open(path, 'w') as file:
        config_object = configparser.ConfigParser()
        sections=config_dict.keys()
        for section in sections:
            config_object.add_section(section)
        for section in sections:
            inner_dict=config_dict[section]
            fields=inner_dict.keys()
            for field in fields:
                value=inner_dict[field]
                config_object.set(section, field, str(value))
        print('writing config to file', path)
        config_object.write(file)


def redun_run_main():
    from redun import Scheduler
    from redun.config import Config

    # enable for PG
    init()
    config = Config(REDUN_CONFIG_VAL)
    scheduler = Scheduler(config=config)
    scheduler.load()
    result = scheduler.run(root())
    result['distinct_files'] = len(result['hashes'])
    del result['hashes']
    print('final result', result)



@task()
def root():
    print('+root')
    return walk([File(ROOT)])[0]



@task()
def walk(paths):
    files = []
    file_size = 0
    dirs = []
    dirs_fut = []
    files_fut = []
    for path in paths:
        path = path.path
        # print(os.getpid(), '+walk', path)
        for item in sorted(os.listdir(path)):
            item = os.path.join(path, item)
            if os.path.isdir(item):
                dirs.append(File(item))
                if len(dirs) > DIR_BATCH_COUNT:
                    dirs_fut.append(walk(dirs))
                    dirs = []
            elif os.path.isfile(item):
                files.append(File(item))
                file_size += os.stat(item).st_size
                if file_size > FILE_BATCH_SIZE_BYTES or len(files) > FILE_BATCH_MAX_COUNT:
                    files_fut.append(handle_files(files))
                    files = []
                    file_size = 0
            else:
                print('UNKNWON THING TYPE: ', item)
    if files:
        files_fut.append(handle_files(files))
        files = []
    if dirs:
        dirs_fut.append(walk(dirs))
        dirs = []
    return walk_combine(paths, dirs_fut, files_fut)


@task(check_valid="shallow")
def walk_combine(paths, dirs, files):
    dirs = sum(dirs, start=[])
    files = sum(files, start=[])
    dir_cnt = sum(d['dir_cnt'] for d in dirs) + len(paths)
    file_cnt = len(files) + sum(d['file_cnt'] for d in dirs)
    file_paths = {}
    file_hashes = set(f['md5'] for f in files)
    for d in dirs:
        file_hashes |= d['hashes']

    return [{
        'path': paths[0],
        'file_cnt': file_cnt,
        'dir_cnt': dir_cnt,
        'hashes': file_hashes,
    }]


@task()
def handle_files(paths):
    return [handle_file(x.path) for x in paths]


def hash_file_py(path):
    import hashlib

    with open(path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def hash_file_bash(path):
    def decode(bytes_):
        return bytes_.decode('utf-8', 'surrogateescape')
    md5 = subprocess.check_output(["md5sum",  decode(path)])
    md5 = md5.split(b' ')[0].decode('ascii')
    return md5


def handle_file(path):
    md5 = hash_file_bash(path)
    return {'md5': md5, 'path': path, 'doc': handle_doc(md5)}


# @task()
def handle_doc(md5):
    # print(os.getpid(), '+handle_doc', md5)
    return {"md5": md5}


def redun_cli():
    from redun.cli import RedunClient, RedunClientError
    client = RedunClient()
    try:
        client.execute()
    except RedunClientError as error:
        print(
            "{error_type}: {error}".format(
                error_type=type(error).__name__,
                error=str(error),
            )
        )
        sys.exit(1)


def main():
    import sys
    write_config_file(REDUN_CONFIG_VAL, REDUN_CONFIG_FILE)
    if len(sys.argv) > 1:
        redun_cli()
    else:
        redun_run_main()


if __name__ == '__main__':
    main()
