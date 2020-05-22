import json
import logging
import os
import sys
import time
from tarfile import TarFile, GNU_FORMAT

import mgzip as mgzip

from utils import numToReadable

process_start_time = time.time()

logger = logging.getLogger(__name__)

logging.basicConfig(format="%(asctime)s [%(levelname)s] - %(message)s", level=logging.INFO)

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

    logger.info("About to archive %s games for %s" % (len(source_dirs), game))

    source_base = item.get("source_base", None)
    destination = item.get("dest_dir", None)
    if not source_base or not destination:
        continue

    source_base = source_base.replace("/", os.path.sep)
    source_base = os.path.expanduser(source_base)
    source_split = source_base + os.path.sep

    destination = destination.replace("/", os.path.sep)
    destination = os.path.realpath(os.path.join(dest_base, destination))

    for source_dir in source_dirs:
        tarfile = os.path.join(temp_dir, source_dir + ".tar")
        destination_file = os.path.join(destination, source_dir + ".tar.gz")
        source = os.path.join(source_base, source_dir)

        logger.info("Archive source: %s" % source)
        logger.info("Temporary archive (uncompressed): %s" % tarfile)
        logger.info("Archive destination: %s" % destination_file)

        file_count = 0
        orig_size = 0
        start_time = time.time()
        to_archive = {}
        for (root, dirs, files) in os.walk(source, topdown=True):
            for file_ in files:
                filename = os.path.join(root, file_)
                arcfile = filename.split(source_split)[1]
                orig_size += os.path.getsize(filename)
                to_archive[filename] = arcfile

        total_files = len(to_archive)
        logger.info("Files to archive: %s (%sB)" % (total_files, numToReadable(orig_size)))

        with TarFile(tarfile, "w", format=GNU_FORMAT) as my_tar:
            for (filename, arcfile) in sorted(to_archive.items()):
                file_count += 1
                if file_count % 100 == 0:
                    logger.info("Progress: %s/%s files written" % (file_count, total_files))
                my_tar.add(filename, arcfile)

        end_time = time.time()
        duration = end_time - start_time

        logger.info("Archived %s files (%sB) in %.3fs" % (file_count, numToReadable(orig_size), duration))

        logger.info("Compressing (%s threads) %s to %s" % (threads, tarfile, destination_file))
        start_time = time.time()
        blocksize = 10 * 2**20
        with mgzip.open(destination_file, "wb", thread=threads, blocksize=blocksize) as my_gzip:
            with open(tarfile, "rb") as my_tar:
                my_gzip.write(my_tar.read())

        end_time = time.time()
        duration = end_time - start_time

        out_size = os.path.getsize(destination_file)
        logger.info("Compressed %sB into %sB in %.3fs" % (numToReadable(orig_size), numToReadable(out_size), duration))

        os.unlink(tarfile)

process_end_time = time.time()
duration = process_end_time - process_start_time
logger.info("Entire archiving time: %.3fs" % duration)