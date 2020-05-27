import json
import logging
import os
import re
import sys
import time
from tarfile import TarFile, GNU_FORMAT

import mgzip as mgzip

from utils import numToReadable, generate_sha1sum, generate_exclude_dirs, generate_manifest, load_manifest_file, \
    write_manifest_file

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

    exclude_dirs = generate_exclude_dirs(item)

    source_base = source_base.replace("/", os.path.sep)
    source_base = os.path.expanduser(source_base)
    source_split = source_base + os.path.sep

    destination = destination.replace("/", os.path.sep)
    destination = os.path.realpath(os.path.join(dest_base, destination))

    for source_dir in source_dirs:
        tarfile = os.path.join(temp_dir, source_dir + ".tar")
        destination_file = os.path.join(destination, source_dir + ".tar.gz")
        manifest_file = os.path.join(destination, source_dir + ".manifest.json")
        source = os.path.join(source_base, source_dir)

        logger.info("Archive source: %s" % source)
        logger.info("Temporary archive (uncompressed): %s" % tarfile)
        logger.info("Archive destination: %s" % destination_file)
        logger.info("Manifest file: %s" % manifest_file)

        start_time = time.time()
        manifest = generate_manifest(source_base, source_dir, exclude_dirs)
        orig_size = sum([item.get("size", 0) for item in manifest.values()])
        logger.info("Files to archive: %s (%sB)" % (len(manifest), numToReadable(orig_size)))

        old_manifest = load_manifest_file(manifest_file)

        archive_manifest = {filename: item for (filename, item) in manifest.items()
                            if old_manifest.get(filename, {}).get("size", None) != item.get("size", None)
                            or old_manifest.get(filename, {}).get("sha1", None) != item.get("sha1", None)}

        if not archive_manifest:
            logger.info("Manifest matches what exists, skipping.")
            continue

        orig_size = sum([item.get("size", 0) for item in archive_manifest.values()])

        total_files = len(archive_manifest)
        logger.info("Files to archive (changed files only): %s (%sB)" % (total_files, numToReadable(orig_size)))

        to_archive = {item.get("filename", None): item.get("arcfile", None) for item in archive_manifest.values()}

        logger.info("Archiving to tar file")
        file_count = 0
        with TarFile(tarfile, "w", format=GNU_FORMAT) as my_tar:
            for (filename, arcfile) in sorted(to_archive.items()):
                if not filename or not arcfile:
                    continue

                file_count += 1
                if file_count % 100 == 0:
                    logger.info("Progress: %s/%s files written" % (file_count, total_files))
                my_tar.add(filename, arcfile)

        end_time = time.time()
        duration = end_time - start_time
        rate = orig_size / duration

        logger.info("Archived %s files (%sB) in %.3fs: %sB/s" %
                    (file_count, numToReadable(orig_size), duration, numToReadable(rate)))

        write_manifest_file(manifest_file, manifest)

        start_time = time.time()
        blocksize = int(min(100 * 2**20, orig_size/threads))
        logger.info("Compressing (%s threads, blocksize %sB) %s to %s" %
                    (threads, numToReadable(blocksize), tarfile, destination_file))

        with mgzip.open(destination_file, "wb", thread=threads, blocksize=blocksize) as my_gzip:
            with open(tarfile, "rb") as my_tar:
                my_gzip.write(my_tar.read())

        end_time = time.time()
        duration = end_time - start_time

        out_size = os.path.getsize(destination_file)
        rate = orig_size / duration
        logger.info("Compressed %sB into %sB in %.3fs: %sB/s" %
                    (numToReadable(orig_size), numToReadable(out_size), duration, numToReadable(rate)))

        os.unlink(tarfile)

process_end_time = time.time()
duration = process_end_time - process_start_time
logger.info("Entire archiving time: %.3fs" % duration)