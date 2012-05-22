#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os
import os.path
import errno

class DRDFSMetaDB(object):
    """The class of DRDFS's DB to use in metadata management
    """

    def __init__(self, path):
        self.db_file = "mogami_meta.db"
        self.db_path = os.path.join(path, self.db_file)

        self.db_conn = sqlite3.connect(self.db_path)
        self.db_cur = self.db_conn.cursor()

        # Create files table
        self.db_cur.execute("""
        CREATE TABLE files (
        path TEXT PRIMARY KEY,
        mode INT,
        uid INT,
        gid INT,
        nlink INT,
        size INT,
        atime INT,
        mtime INT,
        ctime INT,
        dist TEXT,
        dist_path TEXT
        )
        """)

