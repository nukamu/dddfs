#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os
sys.path.append(os.pardir)
import thread, datetime, random

from conf import conf
import chooseDataNode

def _define_replication_distination(data_nodes, cluster_info):
    """

    @param data_nodes data servers' list that have file contents now
    @param cluster_info information of cluster
    @return ip of node chosen as a distination
    """
    cluster_dict = {}

    for cluster in cluster_info.cluster_dict.itervalues():
        cluster_dict[cluster] = 0

    for data_node in data_nodes:
        cluster_name = cluster_info.cluster_dict[data_node]
        if not cluster_dict.has_key(cluster_name):
            cluster_dict[cluster_name] = 1
        else:
            cluster_dict[cluster_name] += 1

    candidates = []
    min_replnum = 99999

    # select clusters with minimam data contents now
    # e.g.) candidates = ['huscs', 'hongo']
    for cluster, replnum in cluster_dict.iteritems():
        if min_replnum > replnum:
            min_replnum = replnum
            candidates = []
            candidates.append(cluster)
        elif min_replnum == replnum:
            candidates.append(cluster)

    selected_cluster = candidates[random.randint(0, len(candidates) - 1)]
    if len(candidates) == 0:
        return None
    
    return cluster_info.choose_datanode_in_cluster(selected_cluster, data_nodes)

class ReplicationManager(object):
    """
    @input: A file specifier
    @output: Misc. replication information

    @note:
    - This class does not tell which dataserver has a file.
      Use some other means to get the location.

    - Method to suggest replication removements is not implemented yet.
    """

    def __init__(self, ):
        self.replOpenCnt = {}      # ex: replOpenCnt["some_file"] ->
                                   #       Total number of `open's for "some_file"
        # database


    def _IncReplOpenCnt(self, f):
        if not self.replOpenCnt.has_key(f):
            self.replOpenCnt[f] = 1
        else:
            self.replOpenCnt[f] += 1

        # log to visualize


    def _DecReplOpenCnt(self, f):
        if self.replOpenCnt[f] > 0:
            self.replOpenCnt[f] -= 1
            
        # log to visualize


    def ReplInfoWhenOpen(self, filename, f_from, f_dists, cluster_info):
        """
        @thread-safty
        UNSAFE.
        Be sure to call this function in critical sections.

        @note
        Assumes 2 functions (or data structures).
        - location(f):
        - reqQ(f):

        @parameters
        f: File identifiler (str)
        openCnt: Total number of `open's for f

        @returns
        A list of replication flow (DataNodeInfo->DataNodeInfo):
          {'from': DataNodeInfo, 'to': DataNodeInfo}

        @example
        # In metadata server daemon
        def __init__():
            self.replManInstance = ReplicationManager()  # Unique instance

        def open(f):  # File f is to be opend
            replInfo = self.replManInstance.GetNewReplInfo(f)
            for replToFrom in replInfo:
                replicate(to=replToFrom['to'], from=replToFrom['from'])
        """
        self._IncReplOpenCnt(filename)
        
        to_ip = _define_replication_distination(f_dists, cluster_info)
        if to_ip == None:  # replication will not be happend
            return None

        from_ip = chooseDataNode.chooseDataNode(f_from, f_dists, cluster_info)
        return {'from': from_ip, 'to': to_ip}

    def ReplInfoWhenClose(self, f):
        self._DecReplOpenCnt(f)

