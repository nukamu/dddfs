#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import sys, os, os.path
sys.path.append(os.pardir)

# dddfs's original modules
from libs import channel, dbmng, system, cluster
from libs import ReplicationManager, chooseDataNode
from conf import conf
from libs.system import DRDFSLog

# standard modules
import stat, errno
import threading, Queue
import socket, cPickle, select
import random, string, re, time


class DRDFSMeta(object):
    """Class for DRDFS's meta data server
    """
    def __init__(self, rootpath, dddfs_dir):
        self.rootpath = os.path.abspath(rootpath)
        self.dddfs_dir = dddfs_dir
        
        """Check directory for meta data files.
        """
        if os.access(self.rootpath,
                     os.R_OK and os.W_OK and os.X_OK) == False:
            sys.exit("%s is not permitted to use. " % (self.rootpath, ))

        DRDFSLog.init("meta", DRDFSLog.DEBUG)
        DRDFSLog.info("** DRDFS metadata server init **")
        DRDFSLog.debug("rootpath = " + self.rootpath)

        # for replication
        self.repl_info = ReplicationManager.ReplicationManager()
        self.cluster_info = cluster.DDDFSNodesInfo(
            os.path.join(self.dddfs_dir, 'conf', conf.cluster_conf_file))

        self.access_info = chooseDataNode.FileAccessInfo()

        self.delfiles_q = Queue.Queue()
        self.datalist = []
        self.repq = Queue.Queue()

    def run(self, ):
        """Connected from a client
        """
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.bind(("0.0.0.0", conf.metaport))
        self.lsock.listen(10)

        DRDFSLog.debug("Listening at the port " + str(conf.metaport))
        daemons = []
        collector_thread = self.thread_collector(daemons)
        collector_thread.start()
        threads_count = 0

        delete_files_thread = self.delete_files(self.delfiles_q)
        delete_files_thread.start()
        replicator_thread = self.replicator(self.repq, self.access_info, self.rootpath)
        replicator_thread.start()
        delete_replica_thread = self.del_repl(self.repl_info, self.delfiles_q, self.rootpath)
        delete_replica_thread.start()

        while True:
            (csock, address) = self.lsock.accept()
            c_channel = channel.DRDFSChannel()
            c_channel.set_channel_from_sock(csock)
            DRDFSLog.debug("accept connnect from %s" % (str(address[0])))
            metad = self.handler(c_channel, self.rootpath, self.delfiles_q, self.datalist, self.repq, 
                                 self.repl_info, self.access_info, self.cluster_info)
            threads_count += 1
            metad.start()
            daemons.append(metad)
            
            DRDFSLog.debug("Created thread name = " + metad.getName())

    class delete_files(threading.Thread):
        """Send the data servers the request to delete files.
        
        @param files file list to delete
        """
        def __init__(self, delfiles_q):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.delfiles_q = delfiles_q

        def run(self, ):
            while True:
                try:
                    (target_ip, target_file) = self.delfiles_q.get(timeout=3)
                    self.send_delete_request(target_ip, [target_file, ])
                except Queue.Empty:
                    pass

        def send_delete_request(self, IP, files):
            senddata = ['filedel', files]
            ch = channel.DRDFSChannel()
            ch.connect(IP, conf.dataport)
            ans = ch.send_recv_flow(senddata)
            senddata = ['close']
            ch.send_header(senddata)
            ch.brk_channel()

    class thread_collector(threading.Thread):
        """Collect dead threads in arguments.
        
        @param daemons thread list to collect: alive and dead threads are included
        """
        def __init__(self, daemons):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.daemons = daemons

        def run(self, ):
            while True:
                daemons_alive = threading.enumerate()
                for d in self.daemons:
                    if d not in daemons_alive:
                        d.join()
                        self.daemons.remove(d)
                time.sleep(5)

    class replicator(threading.Thread):
        """Class for the thread making replica.
        """
        def __init__(self, repq, access_info, rootpath):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.repq = repq
            self.sock_list =[]
            self.sock_dict = {}
            self.access_info = access_info
            self.rootpath = rootpath

        def run(self, ):
            while True:
                # check tasks of replication
                try:
                    (rep_from, rep_filename, rep_to,
                     org_filename) = self.repq.get(timeout=1)
                    rep_ch = channel.DRDFSChannel()
                    rep_ch.connect(rep_from, conf.dataport)
                    senddata = ['do_repl', rep_filename, rep_to]
                    rep_ch.send_header(senddata)
                    self.sock_list.append(rep_ch.sock.fileno())
                    self.sock_dict[rep_ch.sock.fileno()] = (rep_ch, org_filename, rep_from, rep_to)
                except Queue.Empty:
                    pass

                # check messages of replication ends
                ch_list = select.select(self.sock_list, [], [], 1)[0]
                for socknum in ch_list:
                    ch = self.sock_dict[socknum][0]
                    org_filename = self.sock_dict[socknum][1]
                    org_dist = self.sock_dict[socknum][2]
                    new_dist = self.sock_dict[socknum][3]
                    (ans, dist_filename, size)  = ch.recv_header()
                    if ans == 0:
                        # add new location of the file
                        f = open(self.rootpath + org_filename, 'a')
                        buf = "%s,%s,%d,%s\n" % (new_dist, dist_filename, size, org_dist)
                        f.write(buf)
                        f.close()
                        print "*** add replication ****"
                        self.access_info.add_dest(org_filename, new_dist)
                    self.sock_list.remove(ch.sock.fileno())
                    del self.sock_dict[ch.sock.fileno()]
                    senddata = ['close', ]
                    ch.send_header(senddata)
                    ch.brk_channel()

    class del_repl(threading.Thread):
        """Class for the thread to delete replica.
        """
        def __init__(self, repl_info, delfiles_q, rootpath):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.repl_info = repl_info
            self.delfiles_q = delfiles_q
            self.rootpath = rootpath

        def run(self, ):
            last_access_time = self.repl_info.last_access_time
            repl_num_dict = self.repl_info.replNum

            while True:
                # check tasks of deleting replica
                for filename, last_access in last_access_time.iteritems():
                    if time.time() - last_access > conf.repl_delete_time:
                        if repl_num_dict[filename] == 1:
                            last_access_time[filename] = time.time()
                            continue
                        org_size = os.path.getsize(self.rootpath + filename)
                        f = open(self.rootpath + filename, 'r+')
                        lines = f.readlines()
                        if len(lines) > 1:
                            f.truncate(org_size - len(lines[-1]))
                            l = lines[-1].rsplit(",")
                            if len(l) >= 2:
                                self.delfiles_q.put((l[0], l[1]))
                        f.close()
                        last_access_time[filename] = time.time()
                time.sleep(5)


    class handler(threading.Thread):
        """Class for thread created for each client
        This handler is run as multithread.
        """
        def __init__(self, c_channel, rootpath, delfiles_q, 
                     datalist, repq, 
                     repl_info, access_info, cluster_info):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.c_channel = c_channel
            self.rootpath = rootpath
            self.datalist = datalist
            self.delfiles_q = delfiles_q
            self.repq = repq
            self.repl_info = repl_info
            
            self.access_info = access_info
            self.cluster_info = cluster_info

        def run(self, ):
            while True:
                header = self.c_channel.recv_header()
                if header == None:
                    DRDFSLog.debug("Connection closed")
                    break

                if header[0] == 'getattr':
                    DRDFSLog.debug("** getattr **")
                    self.getattr(self.rootpath + header[1])

                elif header[0] == 'readdir':
                    DRDFSLog.debug("** readdir **")
                    self.readdir(self.rootpath + header[1])

                elif header[0] == 'access':
                    DRDFSLog.debug("** access **")
                    self.access(self.rootpath + header[1],
                                header[2])

                elif header[0] == 'mkdir':
                    DRDFSLog.debug("** mkdir **")
                    self.mkdir(self.rootpath + header[1], 
                               header[2])

                elif header[0] == 'rmdir':
                    self.rmdir(self.rootpath + header[1])

                elif header[0] == 'unlink':
                    self.unlink(self.rootpath + header[1])

                elif header[0] == 'rename':
                    self.rename(self.rootpath + header[1],
                                self.rootpath + header[2])
                elif header[0] == 'mknod':
                    self.mknod(self.rootpath + header[1],
                               header[2], header[3])

                elif header[0] == 'chmod':
                    self.chmod(self.rootpath + header[1],
                               header[2])

                elif header[0] == 'chown':
                    self.chown(self.rootpath + header[1],
                               header[2], header[3])

                elif header[0] == 'link':
                    self.link(self.rootpath + header[1],
                              self.rootpath + header[2])

                elif header[0] == 'symlink':
                    self.symlink(self.rootpath + header[1],
                                 self.rootpath + header[2])

                elif header[0] == 'readlink':
                    self.readlink(self.rootpath + header[1])

                elif header[0] == 'truncate':
                    self.truncate(self.rootpath + header[1],
                                  header[2])

                elif header[0] == 'utime':
                    self.utime(self.rootpath + header[1],
                               header[2])

                elif header[0] == 'fsync':
                    self.fsync(self.rootpath + header[1],
                               header[2])

                elif header[0] == 'open':
                    self.open(header[1], header[2], header[3])

                elif header[0] == 'release':
                    self.release(header[1], header[2], header[3], 
                                 header[4], header[5])

                elif header[0] == 'fgetattr':
                    self.fgetattr(header[1])

                elif header[0] == 'ftruncate':
                    print '** ftruncate'
                    print 'error!'

                elif header[0] == 'dataadd':
                    DRDFSLog.debug('** dataadd **')
                    l = self.c_channel.sock.getpeername()
                    """l[0] is IP address of data server to add.
                    """
                    self.data_add(l[0])

                elif header[0] == 'datadel':
                    DRDFSLog.debug('** datadel **')
                    l = self.c_channel.sock.getpeername()
                    self.data_del(l[0])

                else:
                    print '** this is unexpected header. break!'
                    exit

        # System APIs
        def data_add(self, IP):
            """
            """
            ret = self.cluster_info.add_node(IP, cluster.NodeInfo.TYPE_DATA)
            self.c_channel.send_header(ret)
            if ret == 0:
                self.datalist.append(IP)
                print "add data server IP:", IP
                print "Now %d data servers are.." % len(self.datalist)

        def data_del(self, IP):
            """
            """
            for l in self.datalist:
                if l == IP:
                    self.datalist.remove(l)
                    break
            else:
                sys.exit("not exist such a data server (IP:%s)" % IP)
            print "delete data server IP:", IP
            print "Now there are %d data servers." % len(self.datalist)

        # DRDFS's actual metadata access APIs
        def getattr(self, path):
            DRDFSLog.debug('path = ' + path)
            try:
                st = os.lstat(path)
            except os.error, e:
                DRDFSLog.debug("stat error!")
                senddata = [e.errno, "null"]
                self.c_channel.send_header(senddata)
                return
            if os.path.isfile(path):
                f = open(path, 'r')
                buf = f.readline()
                f.close()
                l = buf.rsplit(',')
                try:
                    senddata = [0, st, string.atol(l[2])]
                except Exception, e:
                    print "len(l) = %d" % (len(l))
                    raise
            else:
                senddata = [0, st, -1]
            self.c_channel.send_header(senddata)

        def readdir(self, path):
            DRDFSLog.debug('path=%s' % (path))
            try:
                l = os.listdir(path)
                senddata = [0, l]
            except os.error, e:
                senddata = [e.errno, "null"]
                print 'error!'
            self.c_channel.send_header(senddata)

        def access(self, path, mode):
            DRDFSLog.debug('path=%s' % (path))
            try:
                if os.access(path, mode) == True:
                    senddata = True
                else:
                    senddata = False
            except os.error, e:
                senddata = False
            self.c_channel.send_header(senddata)

        def mkdir(self, path, mode):
            DRDFSLog.debug('path=%s mode=%o' % (path, mode))
            try:
                os.mkdir(path, mode)
                senddata = 0
            except os.error, e:
                senddata = e.errno
            self.c_channel.send_header(senddata)

        def rmdir(self, path):
            DRDFSLog.debug('path=%s' % (path))
            try:
                os.rmdir(path)
                senddata = 0
            except os.error, e:
                senddata = e.errno
            self.c_channel.send_header(senddata)
            
        def unlink(self, path):
            DRDFSLog.debug(path)
            if os.path.isfile(path):
                f = open(path, 'r')
                while True:
                    buf = f.readline()
                    if buf == '':
                        break
                    l = buf.rsplit(',')
                    self.delfiles_q.put((l[0], l[1]))
                self.access_info.del_file(path)
                f.close()
            try:
                os.unlink(path)
                senddata = 0
            except os.error, e:
                senddata = e.errno
            self.c_channel.send_header(senddata)

        def rename(self, oldpath, newpath):
            DRDFSLog.debug(oldpath + ' -> ' + newpath)
            try:
                os.rename(oldpath, newpath)
                senddata = 0
            except os.error, e:
                senddata = e.errno
            self.c_channel.send_header(senddata)

        def chmod(self, path, mode):
            DRDFSLog.debug('path=%s w/ mode %o' % (path, mode))
            try:
                os.chmod(path, mode)
                senddata = 0
            except os.error, e:
                senddata = e.errno
            self.c_channel.send_header(senddata)

        def chown(self, path, uid, gid):
            DRDFSLog.debug("path=%s uid=%d gid=%d" % (path, uid, gid))
            try:
                os.chown(path, uid, gid)
                senddata = 0
            except os.error, e:
                senddata = e.errno
            self.c_channel.send_header(senddata)

        def truncate(self, path, len):
            """truncate handler.

            @param path file path
            @param len length of output file
            """
            DRDFSLog.debug("path = %s, len = %d" % (path, len))
            try:
                f = open(path, 'r+')
                filedist_info_list = []
                while True:
                    # read original data
                    buf = f.readline()
                    if buf == '':  # EOF
                        break
                    l = buf.rsplit(',')
                    if len(l) == 3:
                        filedist_info_list.append((l[0], l[1]))          
                    elif len(l) == 4:
                        filedist_info_list.append((l[0], l[1], l[3]))
                    else: 
                        break
                f.truncate(0)
                f.seek(0)

                for filedist_info in filedist_info_list:
                    dist = filedist_info[0]
                    filename = filedist_info[1]
                    if len(filedist_info) == 2:
                        filesource = ''
                    else: 
                        filesource = filedist_info[2]
                    f.write("%s,%s,%d,%s\n" % (dist, filename, len, filesource))
                f.close()
                senddata = [0, filedist_info_list]
            except IOError, e:
                senddata = [e.errno, ]
            except Exception, e:
                senddata = [e.errno, ]
            self.c_channel.send_header(senddata)

        def utime(self, path, times):
            DRDFSLog.debug("path = %s, times = %s" % (path, times))
            try:
                os.utime(path, times)
                senddata = 0
            except os.error, e:
                senddata = e.errno
            self.c_channel.send_header(senddata)

        def open(self, path, flag, mode):
            """open handler.

            @param path file path
            @param flag flags for open(2)
            @param mode open mode (may be empty tuple): actual value is mode[0]
            """
            if os.access(self.rootpath + path, os.F_OK) == True:
                """When the required file exist...
                """
                try:
                    DRDFSLog.debug("!!find the file %s w/ %o" % (path, flag))
                    created = False
                    fd = 0
                    if mode:
                        fd = os.open(self.rootpath + path, os.O_RDWR, mode[0])
                    else:
                        fd = os.open(self.rootpath + path, os.O_RDWR)
                        DRDFSLog.debug("fd = %d" % (fd))
                    data_buf = ""
                    while True:
                        buf = os.read(fd, conf.bufsize)
                        if buf == '':
                            break
                        data_buf += buf
                    lines = data_buf.rsplit('\n')
                    data_nodes = []
                    filename = ''
                    for line in lines:
                        l = line.rsplit(',')
                        if len(l) < 3:
                            break
                        data_nodes.append(l[0])  # e.g.) "127.0.0.1"
                        filename = l[1]
                        size = string.atol(l[2])
                    data_num = len(lines)
                    peername = self.c_channel.sock.getpeername()[0]

                    print path
                    dest = chooseDataNode.ChooseDataNode(path, peername, data_nodes, 
                                                              self.access_info, self.cluster_info)
                    senddata = [0, dest, fd, size, filename, created]
                    self.access_info.add_load(path, dest)
                except os.error, e:
                    DRDFSLog.debug("!!find the file but error for %s (%s)" % (path, e))
                    senddata = [e.errno, None, None, None, None, None]
                self.c_channel.send_header(senddata)

                # do replication (if need)
                repl_dict = self.repl_info.ReplInfoWhenOpen(path, data_nodes, 
                                                            self.access_info, self.cluster_info)
                if repl_dict != None:                   
                    # add a replication task
                    if conf.replication == True:
                        # select distination in replication
                        self.repq.put((repl_dict['from'], filename, repl_dict['to'], path))
                        print "task ***"


            else:
                """When the required file doesn't exist...
                """
                DRDFSLog.debug("can't find the file so create!!")
                created = True
                try:
                    fd = 0
                    if mode:
                        fd = os.open(self.rootpath + path, os.O_RDWR | os.O_CREAT, mode[0])
                    else:
                        fd = os.open(self.rootpath + path, os.O_RDWR | os.O_CREAT)
                except os.error, e:
                    print "!! have fatal error @1!! (%s)" % (e)
                    raise
                try:
                    rand = 0
                    if len(self.datalist) > 0:
                        rand = random.randint(0, len(self.datalist) - 1)
                    else:
                        DRDFSLog.error("!! There are no data server to create file !!")
                    dist = self.datalist[rand]

                    # make a filename in a random manner
                    filename = ''.join(random.choice(string.letters) for i in xrange(16))
                    DRDFSLog.debug("filename is %s" % (filename,))
                    buf = dist + ',' + filename + ',' + '0,' + '\n'
                except Exception, e:
                    print "!! have fatal error @2!! (%s)" % (e)
                    raise
                DRDFSLog.debug("fd = " + str(fd))
                try:
                    os.write(fd, buf)
                    os.fsync(fd)
                except os.error, e:
                    print "!! have fatal error @3!! (%s)" % (e)
                    raise
                self.access_info.add_file(path, dist)
                
                size = 0
                senddata = [0, dist, fd, size, filename, created]
                self.c_channel.send_header(senddata)

                
        def release(self, fd, dest, fsize, filename, created):
            """release handler.

            @param fd file discripter
            @param writelen size of data to be written
            """
            os.lseek(fd, 0, os.SEEK_SET)
            DRDFSLog.debug("fd = " + str(fd))
            if created == False:
                self.repl_info.ReplInfoWhenClose(filename, dest, 
                                                 self.access_info)

            else:
                try:
                    buf = os.read(fd, conf.bufsize)
                except os.error, e:
                    print "OSError in release (%s)" %(e)
                l = buf.rsplit(',')
                size = string.atol(l[2])
                try:
                    buf = l[0] + ',' + l[1] + ',' + str(fsize) + ',\n'
                    DRDFSLog.debug("write to meta file %s" % buf)
                    
                    os.ftruncate(fd, len(buf))
                    os.lseek(fd, 0, os.SEEK_SET)
                    os.write(fd, buf)
                    os.fsync(fd)
                except os.error, e:
                    print "OSError in release (%s)" %(e)
            os.close(fd)
            senddata = 0
            self.c_channel.send_header(senddata)
            
        def fgetattr(self, fd):
            print fd
            try:
                st = os.fstat(fd)
                senddata = [0, st]
            except os.error, e:
                print "OSError in fgetattr (%s)" % (e)
                senddata = [e.errno, 'null']
            self.c_channel.send_header(senddata)


def main(dir_path, dddfs_dir):
    meta = DRDFSMeta(dir_path, dddfs_dir)
    meta.run()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("Usage: ")
    meta = DRDFSMeta(sys.argv[1])
    meta.run()

