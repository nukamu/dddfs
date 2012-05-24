FETCH_COST_ULIMIT = 9999999999

import random

def _FetchCost(latency, load):
    cost = latency * load
    assert(cost < FETCH_COST_ULIMIT)
    return cost

def ChooseDataNode(from_node, data_candidates, clustersInfo):
    """
    @parameters
    targetFile: Unique file specifier
    clustersInfo: List of each cluster information

    @returns
    DataNodeInfo

    @example
    # In metadata server daemon
    def open():
        # when asked by an intermediate node (interNode) to fetch a file (f)
        dataNodeToFetch = ChooseDataNode(interNode, f, clustersInfo)  # Assuming `clustersInfo' is accessible
    """
    ## for test
    # return a data node from candidates at random
    return data_candidates[random.randint(0, len(data_candidates) - 1)]

    minCost = FETCH_COST_ULIMIT
    minCostIdx = -1

    # Search dataNode with minimum fetching cost
    for idx, dataNodeCandidate in enumerate(dataNodeCandidates):
        candidateCluster = clusterInfos[dataNodeCandidates.cluster]

        #rtt = intermediateNodeToFetchTargetFile.cluster.RTT[candidateCluster]
        cost = _FetchCost(rtt, dataNode.load)
        if cost < minCost:
            minCost = cost
            minCostIdx = idx
    return dataNodeCandidates[minCostIdx]
