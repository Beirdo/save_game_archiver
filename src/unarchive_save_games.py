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

    logger.info("About to unarchive %s games for %s" % (len(source_dirs), game))

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
        destination_file = os.path.join(destination, source_dir + ".tar.gz")
        source = os.path.join(source_base, source_dir)

        logger.info("Archive source: %s" % destination_file)
        logger.info("Archive destination: %s" % source)

        start_time = time.time()
        with mgzip.open(destination_file, "rb", thread=threads) as my_gzip:
            with TarFile(fileobj=my_gzip) as my_tar:
                my_tar.extractall(path=source_base)

        end_time = time.time()
        duration = end_time - start_time

        logger.info("Unarchived %s in %.3fs" % (destination_file, duration))

process_end_time = time.time()
duration = process_end_time - process_start_time
logger.info("Entire archiving time: %.3fs" % duration)