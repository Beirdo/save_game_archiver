def numToReadable(value):
    prefixes = ["", "k", "M", "G", "T", "P"]
    index = 0
    for (index, prefix) in enumerate(prefixes):
        if value <= 700.0:
            break
        value /= 1024.0
    return "%.2f%s" % (value, prefixes[index])
