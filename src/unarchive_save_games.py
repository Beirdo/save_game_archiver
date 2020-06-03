import json
import logging
import os
import re
import sys
import tarfile
import time

from utils import numToReadable, generate_sha1sum, generate_exclude_dirs, generate_manifest, load_manifest_file, \
    write_manifest_file

process_start_time = time.time()

logger = logging.getLogger(__name__)

logging.basicConfig(format="%(asctime)s [%(levelname)s] (%(threadName)s) - %(message)s", level=logging.INFO)

homedir = os.path.expanduser("~")
config_file = os.path.join(homedir, ".archive", "config.json")

with open(config_file, "r") as f:
    config = json.load(f)

logger.info("Loaded config from %s" % config_file)

dest_base = config.get("dest_base", None)
if not dest_base:
    logger.error("Bad configuration: needs dest_base!")
    sys.exit(1)

temp_dir = config.get("temp_dir", None)
if not temp_dir:
    logger.error("Bad configuration: needs temp_dir!")
    sys.exit(1)

threads = config.get("threads", 8)

temp_dir = temp_dir.replace("/", os.path.sep)

games = config.get("games", {})

for (game, item) in games.items():
    source_dirs = item.get("source_dirs", [])

    logger.info("About to unarchive %s games for %s" % (len(source_dirs), game))

    source_base = item.get("source_base", None)
    destination = item.get("dest_dir", None)
    if not source_base or not destination:
        continue

    exclude_dirs = generate_exclude_dirs(item)

    source_base = source_base.replace("/", os.path.sep)
    source_base = os.path.expanduser(source_base)

    destination = destination.replace("/", os.path.sep)
    destination = os.path.realpath(os.path.join(dest_base, destination))

    for source_dir in source_dirs:
        destination_file = os.path.join(destination, source_dir + ".tar.gz")
        in_manifest_file = os.path.join(destination, source_dir + ".archived.manifest.json")
        out_manifest_file = os.path.join(destination, source_dir + ".unarchived.manifest.json")
        source = os.path.join(source_base, source_dir)

        logger.info("Archive source: %s" % destination_file)
        logger.info("Archive destination: %s" % source)
        logger.info("Archived Manifest file: %s" % in_manifest_file)
        logger.info("Unarchived Manifest file: %s" % out_manifest_file)

        new_manifest = load_manifest_file(in_manifest_file)

        exclude_arcfiles = set()

        if new_manifest:
            old_manifest = generate_manifest(source_base, source_dir, exclude_dirs, threads=threads)
            exclude_arcfiles = {item.get("arcfile", None).replace(os.path.sep, "/")
                                for (filename, item) in old_manifest.items()
                                if new_manifest.get(filename, {}).get("sha1sum", None) == item.get("sha1sum", None)
                                and new_manifest.get(filename, {}).get("size", None) == item.get("size", None)
                                and "arcfile" in item}

        logger.info("Unchanged files: %s" % len(exclude_arcfiles))

        start_time = time.time()
        with tarfile.open(name=destination_file, mode="r:gz") as my_tar:
            members = my_tar.getmembers()
            logger.info("Files to unarchive pre-filtering: %s" % len(members))
            members = list(filter(lambda x: x.name not in exclude_arcfiles, members))
            logger.info("Files to unarchive after filtering out unchanged files: %s" % len(members))
            if members:
                my_tar.extractall(path=source, members=members)

        end_time = time.time()
        duration = end_time - start_time
        filesize = os.path.getsize(destination_file)
        rate = filesize / duration

        logger.info("Unarchived %s (%sB) in %.3fs (%sB/s)" %
                    (destination_file, numToReadable(filesize), duration, numToReadable(rate)))

        write_manifest_file(out_manifest_file, new_manifest)

process_end_time = time.time()
duration = process_end_time - process_start_time
logger.info("Entire unarchiving time: %.3fs" % duration)
