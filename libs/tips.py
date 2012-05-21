#! /usr/bin/env python
#-*- coding: utf-8 -*-

from __future__ import with_statement

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)
import threading
import time
import sys, os
sys.path.append(os.pardir)

from conf import conf


class DRDFSStat(fuse.Stat):
    attrs = ("st_mode", "st_ino", "st_dev", "st_nlink",
             "st_uid", "st_gid", "st_size",
             "st_atime", "st_mtime", "st_ctime")
    drdfs_attrs = ("st_mode", "st_uid", "st_gid", "st_nlink",
                    "st_size", "st_atime", "st_mtime", "st_ctime")

    def __init__(self, ):
        for attr in self.attrs:
            try:
                setattr(self, attr, 0)
            except AttributeError, e:
                print e
                pass

    def load(self, st):
        for attr in self.attrs:
            try:
                setattr(self, attr, getattr(st, attr))
            except AttributeError, e:
                print e
                pass

    def chsize(self, size):
        self.st_size = size

    def __repr__(self):
        s = ", ".join("%s=%d" % (attr, getattr(self, attr))
                      for attr in self.attrs)
        return "<DRDFSStat %s>" % (s,)

class DRDFSBlock(object):
    """This is the class of block information used in Mogami.
    """

    def __init__(self, ):
        self.state = 0
        """0: not exist
        1: requesting
        2: exist
        """
        self.buf = ""
