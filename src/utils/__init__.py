import hashlib
import json
import logging
import os
import re

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


def generate_manifest(source_base, source_dir, exclude_dirs, old_count=-1):
    source = os.path.join(source_base, source_dir)
    source_split = source_base + os.path.sep

    manifest = {}
    subdir_split = source_split + source_dir + os.path.sep
    logger.info("Generating local manifest to support filtering")
    for (root, dirs, files) in os.walk(source, topdown=True):
        basedirname = (root + os.path.sep).split(subdir_split)[1].rstrip(os.path.sep)
        if not basedirname:
            continue

        if old_count != 0:
            found = list(filter(lambda x: x.search(basedirname), exclude_dirs))
            if found:
                continue

        for file_ in files:
            filename = os.path.join(root, file_)
            arcfile = filename.split(source_split)[1]
            filesize = os.path.getsize(filename)
            manifest[filename] = {
                "filename": filename,
                "arcfile": arcfile,
                "size": filesize,
                "sha1sum": generate_sha1sum(filename),
            }

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
