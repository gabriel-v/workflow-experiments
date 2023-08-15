import ray


import os
import subprocess

ROOT = '/opt/node/collections/testdata/data'
context = ray.init()


def encode(str_):
    return str_.encode('utf-8', 'surrogateescape')


def decode(bytes_):
    return bytes_.decode('utf-8', 'surrogateescape')


@ray.remote
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

    files = list(map(handle_file.remote, files))
    dirs = list(map(walk.remote, dirs))

    dirs = ray.get(dirs)
    files = ray.get(files)

    # return ray.get(walk_combine.remote(path, dirs, files))
    return walk_combine(path, dirs, files)


# @ray.remote
def walk_combine(path, dirs, files):
    dir_cnt = 1 + sum(d['dir_cnt'] for d in dirs)
    file_cnt = len(files) + sum(d['file_cnt'] for d in dirs)
    file_paths = {}

    return {
        'path': path,
        'file_cnt': file_cnt,
        'dir_cnt': dir_cnt,
    }


@ray.remote
def handle_file(path):
    # print(os.getpid(), '+handle_file', path)
    md5 = subprocess.check_output(f"md5sum '{decode(path)}'", shell=True)
    md5 = md5.split(b' ')[0].decode('ascii')
    return {'md5': md5, 'path': path, 'doc': handle_doc.remote(md5)}


@ray.remote
def handle_doc(md5):
    # print(os.getpid(), '+handle_doc', md5)
    return md5


def root():
    print('+root')
    return ray.get(walk.remote(encode(ROOT)))


print(root())
