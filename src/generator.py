import sys

from numpy import random

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Format: %s size seed" % sys.argv[0])
        exit(1)

    size = int(sys.argv[1])
    seed = int(sys.argv[2])

    random.seed(seed)
    f    = open("./random.txt", "w")
    buf  = "%d\n" % size
    f.write(buf)
    for i in range(size // 1000 + 1):
        rand = random.randint(size, size=min(1000, size - i * 1000))
        buf  = ""
        for j in rand:
            buf += "%d\n" % j
        f.write(buf)
    f.close()
