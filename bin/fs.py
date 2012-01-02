#! /usr/bin/env python
# -*- coding: utf-8 -*-

from fuse import Fuse
import fuse
import os, errno


class CPDFS(Fuse):
    """
    """

    def __init__(self, *args, **kw):
        Fuse.__init__(self, *args, **kw)


    def fsinit(self):
        pass

    def getattr(self, path):
        return 0

    def readlink(self, path):
        return 0

    def readdir(self, path):
        return 0

    def unlink(self, path):
        return 0

    def rmdir(self, path):
        return 0

    def symlink(self, path_from, path_to):
        return 0

    def rename(self, path_from, path_to):
        return 0

    def link(self, path_from, path_to):
        return 0

    def chmod(self, path, mode):
        return 0

    def chown(self, path, user, group):
        return 0

    def truncate(self, path, len):
        return 0

    def mknod(self, path, mode, dev):
        return 0

    def mkdir(self, path, mode):
        return 0

    def utime(self, path, times):
        return 0

    def access(self, path, mode):
        return 0


    class CPDFSFile(object):
        def __init__(self, path, flags, *mode):
            pass

        def read(self, length, offset):
            return 0

        def write(self, buf, offset):
            return 0

        def release(self, flags):
            return 0

        def fsync(self, isfsyncfile):
            return 0

        def flush(self):
            return 0

        def fgetattr(self):
            return 0

        def ftruncate(self, len):
            return 0
        

    def main(self, *a, **kw):
        self.file_class = self.CPDFSFile
        return Fuse.main(self, *a, **kw)


def main():


if __name__ == '__main__':
    
