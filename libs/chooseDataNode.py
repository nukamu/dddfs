FETCH_COST_ULIMIT = 9999999999


def _FetchCost(latency, load):
    cost = latency * load
    assert(cost < FETCH_COST_ULIMIT)
    return cost

def _ChooseBestDataNode(nodes_info, dataNodeCandidates, clustersInfo):
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


def ChooseDataNode(intermediateNodeToFetchTargetFile, targetFile, clustersInfo):
    """
    @thread-safty
    Garanteed

    @parameters
    intermediateNodeToFetchTargetFile: Instance of IntermediateNodeInfo
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
    currentLocations = f(targetFile) # DataNodeInfo[]
                                     # currentLocations[idx].load
    bestDataNodeInfo = _ChooseBestDataNode(intermediateNodeToFetchTargetFile,
                                           currentLocations, clustersInfo)
    return bestDataNodeInfo

