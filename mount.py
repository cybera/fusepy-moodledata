#!/usr/bin/env python

from moodledata import Moodledata
from sys import argv, exit
from fuse import FUSE

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s <root> <mountpoint>' % argv[0])
        exit(1)

    fuse = FUSE(Moodledata(argv[1]), argv[2], foreground=True)