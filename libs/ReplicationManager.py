import threa
import datetime


class ReplicationManager(object):
    """
    @input: A file specifier
    @output: Misc. replication information

    @note:
    - This class does not tell which dataserver has a file.
      Use some other means to get the location.

    - Method to suggest replication removements is not implemented yet.
    """

    def __init__():
        # TODO: These data structures comsume O(f) space
        self.replOpenCnt = {}      # ex: replOpenCnt["some_file"] ->
                                   #       Total number of `open's for "some_file"

    def _IncReplOpenCnt(f):
        if not self.replOpenCnt.has_key(f):
            self.replOpenCnt = 1
        else:
            self.replOpenCnt += 1


    def GetNewReplInfoWhenOpen(f):
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
        self._IncReplOpenCnt(f)

        # TODO: Determin replication request from location(f) and reqQ(f)
