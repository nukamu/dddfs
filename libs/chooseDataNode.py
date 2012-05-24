#! /usr/bin/env python
# -*- coding: utf-8 -*- 

from __future__ import with_statement

import os, sys
sys.path.append(os.pardir)
import random, threading, sqlite3

from conf import conf

FETCH_COST_ULIMIT = 9999999999

def _FetchCost(latency, load):
    load += 1
    cost = latency * load
    assert(cost < FETCH_COST_ULIMIT)
    return cost

def ChooseDataNode(filename, from_node, data_candidates, fileAccessInfo, clusterInfo):
    """Choose a best data node from candidates to read file from from_node

    @param from_node ip of node requiring the file
    @param data_candidates list of candidates
    @param clusterInfo information of clusters
    @returns selected data node's ip

    @example
    # In metadata server daemon
    def open():
        # when asked by an intermediate node (interNode) to fetch a file (f)
        dataNodeToFetch = ChooseDataNode(interNode, f, clustersInfo)  # Assuming `clustersInfo' is accessible
    """

    if len(data_candidates) == 1:
        return data_candidates[0]

    minCost = FETCH_COST_ULIMIT

    selected_candidates = []
    # Search dataNode with minimum fetching cost
    for data in data_candidates:
        load = fileAccessInfo.access_load(filename, data)
        rtt = clusterInfo.calcRTT(data, from_node)
        cost = _FetchCost(rtt, load)
        print "cost: %s is calced %f" % (data, cost)
        if cost < minCost:
            minCost = cost
            selected_candidates = [data, ]
        elif cost == minCost: 
            selected_candidates.append(data)       

    return selected_candidates[random.randint(0, len(selected_candidates) - 1)]

class AccessInfo():
    def __init__(self, filename, dest):
        self.lock = threading.Lock()
        self.filename = filename
        self.dest_load = {}

        with self.lock:
            self.dest_load[dest] = 0

    def _add_dest(self, new_dest):
        with self.lock:
            self.dest_load[new_dest] = 0

    def _add_load(self, dest_ip):
        with self.lock:
            if not self.dest_load.has_key(dest_ip):
                self.dest_load[dest_ip] = 0
            self.dest_load[dest_ip] += 1

    def _del_load(self, dest_ip):
        with self.lock:
            if not self.dest_load.has_key(dest_ip):
                self.dest_load[dest_ip] = 0
            self.dest_load[dest_ip] -= 1
            
    def _ret_load(self, dest_ip):
        with self.lock:
            if not self.dest_load.has_key(dest_ip):
                self.dest_load[dest_ip] = 0
            return self.dest_load[dest_ip]

class FileAccessInfo():
    def __init__(self, ):
        self.file_dict = {}

        self.db_path = conf.access_db_path
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_cur = self.db_conn.cursor()

        # Create files table
        self.db_cur.execute("""
        CREATE TABLE access_table (
        trace_file TEXT,
        data_node TEXT, 
        num_access INT 
        )
        """)

    def add_file(self, filename, dest_ip):
        self.file_dict[filename] = AccessInfo(filename, dest_ip)

    def add_dest(self, filename, dest_ip):
        print " ** add dest filename = %s, dest_ip = %s" % (filename, dest_ip)
        if not self.file_dict.has_key(filename):
            self.add_file(filename, dest_ip)
        self.file_dict[filename]._add_dest(dest_ip)

    def add_load(self, filename, dest_ip):
        db_conn = sqlite3.connect(self.db_path)
        db_cur = db_conn.cursor()

        db_cur.execute("""SELECT count(*) FROM access_table 
                       WHERE trace_file=? AND data_node=?;""", 
                       (filename, dest_ip))
        r = int(db_cur.fetchone()[0])
        if r == 0:
            print "create"
            db_cur.execute("""
            INSERT INTO access_table 
            VALUES (?, ?, ?)
            """, (filename, dest_ip, 1))
        else:
            print "update (add)"
            db_cur.execute("""
            UPDATE access_table SET num_access = num_access + 1 
            WHERE trace_file like ? AND data_node=?;
            """, (filename, dest_ip))

            
        db_conn.commit()
        db_cur.close()
        db_conn.close()

        if not self.file_dict.has_key(filename):
            self.add_file(filename, dest_ip)
        self.file_dict[filename]._add_load(dest_ip)
        print "add load filename = %s, dest_ip = %s, load = %d" % (filename, dest_ip, self.file_dict[filename].dest_load[dest_ip])


    def del_load(self, filename, dest_ip):
        db_conn = sqlite3.connect(self.db_path)
        db_cur = db_conn.cursor()

        db_cur.execute("""SELECT count(*) FROM access_table 
                       WHERE trace_file=? AND data_node=?;""", 
                       (filename, dest_ip))
        r = int(db_cur.fetchone()[0])
        if r != 0:
            print "update (reduce)"
            db_cur.execute("""
            UPDATE access_table SET num_access = num_access - 1 
            WHERE trace_file like ? AND data_node=?;
            """, (filename, dest_ip))

            
        db_conn.commit()
        db_cur.close()
        db_conn.close()

        if self.file_dict.has_key(filename):
            self.file_dict[filename]._del_load(dest_ip)
        print "del load filename = %s, dest_ip = %s, load = %d" % (filename, dest_ip, self.file_dict[filename].dest_load[dest_ip])
        
    def del_file(self, filename):
        if self.file_dict.has_key(filename):
            del self.file_dict[filename]

    def access_load(self, filename, dest_ip):
        if not self.file_dict.has_key(filename):
            self.add_file(filename, dest_ip)
        return self.file_dict[filename]._ret_load(dest_ip)
