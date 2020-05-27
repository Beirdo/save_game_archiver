import hashlib


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
