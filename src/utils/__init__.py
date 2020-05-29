import concurrent
import hashlib
import json
import logging
import os
import re
import time
from concurrent.futures import ALL_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor

logger = logging.getLogger(__name__)


def numToReadable(value):
    prefixes = ["", "k", "M", "G", "T", "P"]
    index = 0
    for (index, prefix) in enumerate(prefixes):
        if value <= 700.0:
            break
        value /= 1024.0
    return "%.2f%s" % (value, prefixes[index])


def generate_sha1sum(filename):
    blocksize = 1 * 2**20   # 1MB
    h = hashlib.sha1()
    with open(filename, "rb") as f:
        while True:
            chunk = f.read(blocksize)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def generate_exclude_dirs(item):
    path_sep = os.path.sep.replace("\\", "\\\\")
    exclude_dirs = {re.compile(r'^%s([%s].*)?$' % (dir_.replace("/", path_sep), path_sep))
                    for dir_ in item.get("exclude_dirs", [])}
    return exclude_dirs


def generate_manifest_for_file(item):
    filename = item.get("filename", None)
    if filename:
        item["size"] = os.path.getsize(filename)
        item["sha1sum"] = generate_sha1sum(filename)


def generate_manifest(source_base, source_dir, exclude_dirs, old_count=-1, threads=8):
    source = os.path.join(source_base, source_dir)
    source_split = source_base + os.path.sep

    manifest = {}
    subdir_split = source_split + source_dir + os.path.sep
    start_time = time.time()
    logger.info("Generating local manifest to support filtering")
    for (root, dirs, files) in os.walk(source, topdown=True):
        basedir_name = (root + os.path.sep).split(subdir_split)[1].rstrip(os.path.sep)
        if not basedir_name:
            continue

        if old_count != 0:
            found = list(filter(lambda x: x.search(basedir_name), exclude_dirs))
            if found:
                continue

        for file_ in files:
            filename = os.path.join(root, file_)
            arcfile = filename.split(source_split)[1]
            manifest[filename] = {
                "filename": filename,
                "arcfile": arcfile,
            }

    file_count = len(manifest)

    end_time = time.time()
    duration = end_time - start_time
    logger.info("Generating file list (%s files) took %.3fs" % (file_count, duration))

    threads = min(threads, os.cpu_count() - 2)
    start_time = time.time()
    logger.info("Starting SHA1 fingerprinting with %s threads", threads)
    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix="Worker-") as executor:
        futures = {executor.submit(generate_manifest_for_file, item) for item in manifest.values()}
        concurrent.futures.wait(futures, return_when=ALL_COMPLETED)

    end_time = time.time()
    duration = end_time - start_time
    logger.info("SHA1 fingerprinting %s files took %.3fs" % (file_count, duration))

    return manifest


def load_manifest_file(filename):
    logger.info("Reading manifest file: %s" % filename)
    try:
        with open(filename, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error("Error loading manifest file: %s" % e)
        return {}


def write_manifest_file(filename, manifest):
    logger.info("Writing manifest file: %s" % filename)
    with open(filename, "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
