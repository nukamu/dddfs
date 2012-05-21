#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import sys, os, os.path
sys.path.append(os.pardir)

# dddfs's original modules
from libs import channel, dbmng, system
from conf import conf
from libs.system import DRDFSLog

# standard modules
import stat, errno
import threading, Queue
import socket, cPickle, select
import random, string, re, time

class DRDFSNodeInfo(object):

    def __init__(self, IP, cluster):
        self.IP = IP
        self.cluster = cluster

class DRDFSDataNodeInfo(DRDFSNodeInfo):
    
    def __init__(self, IP, cluster, dataPath):
        DRDFSNode.__init__(IP, cluster)
        self.load = 0
             
class DRDFSMetaDataInfo():
    def __init__(self, ):
        pass

class DRDFSMetaFile():   
    def __init__(self, path):
        self.path = path
 
    def get_location(self, ):
        pass

    def update_info(self, ):
        pass




class DRDFSMeta(object):
    """Class for DRDFS's meta data server
    """
    def __init__(self, rootpath):
        self.rootpath = os.path.abspath(rootpath)
        
        """Check directory for meta data files.
        """
        if os.access(self.rootpath,
                     os.R_OK and os.W_OK and os.X_OK) == False:
            sys.exit("%s is not permitted to use. " % (self.rootpath, ))

        DRDFSLog.init("meta", DRDFSLog.DEBUG)
        DRDFSLog.info("** DRDFS metadata server init **")
        DRDFSLog.debug("rootpath = " + self.rootpath)
        
        self.delfiles_dict = {}
        self.delfiles_dict_lock = threading.Lock()
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

        delete_files_thread = self.delete_files(self.delfiles_dict, 
                                                self.delfiles_dict_lock)
        delete_files_thread.start()
        replicator_thread = self.replicator(self.repq)
        replicator_thread.start()

        while True:
            (csock, address) = self.lsock.accept()
            c_channel = channel.DRDFSChannel()
            c_channel.set_channel_from_sock(csock)
            DRDFSLog.debug("accept connnect from %s" % (str(address[0])))
            metad = self.handler(c_channel, self.rootpath, self.delfiles_dict, 
                                 self.delfiles_dict_lock, self.datalist, self.repq)
            threads_count += 1
            metad.start()
            daemons.append(metad)
            
            DRDFSLog.debug("Created thread name = " + metad.getName())

    class delete_files(threading.Thread):
        """Send the data servers the request to delete files.
        
        @param files file list to delete
        """
        def __init__(self, delfiles_dict, lock):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.delfiles_dict = delfiles_dict
            self.delfiles_dict_lock = lock

        def run(self, ):
            while True:
                with self.delfiles_dict_lock:
                    del_key_list = []
                    for IP, files in self.delfiles_dict.iteritems():
                        self.send_delete_request(IP, files)
                        del_key_list.append(IP)
                    for del_key in del_key_list:
                        del self.delfiles_dict[del_key]
                time.sleep(3)

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
        def __init__(self, repq):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.repq = repq
            self.sock_list =[]
            self.sock_dict = {}

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
                    self.sock_dict[rep_ch.sock.fileno()] = (rep_ch, org_filename, rep_to)
                except Queue.Empty:
                    pass

                # check messages of replication ends
                ch_list = select.select(self.sock_list, [], [], 1)[0]
                for socknum in ch_list:
                    ch = self.sock_dict[socknum][0]
                    org_filename = self.sock_dict[socknum][1]
                    new_dist = self.sock_dict[socknum][2]
                    ans = ch.recv_header()
                    if ans == 0:
                        # TODO: add new location of the file
                        f = open(org_filename, 'r+')
                        buf = f.read()
                        l = buf.rsplit(',')
                        buf = "%s:%s,%s,%s" % (l[0], new_dist, l[1], l[2])
                        f.truncate(0)
                        f.seek(0)
                        f.write(buf)
                        f.close()
                        print "*** add replication ****"
                    self.sock_list.remove(ch.sock.fileno())
                    del self.sock_dict[ch.sock.fileno()]
                    senddata = ['close', ]
                    ch.send_header(senddata)
                    ch.brk_channel()

    class handler(threading.Thread):
        """Class for thread created for each client
        This handler is run as multithread.
        """
        def __init__(self, c_channel, rootpath, delfiles_dict, 
                     delfiles_dict_lock, datalist, repq):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.c_channel = c_channel
            self.rootpath = rootpath
            self.datalist = datalist
            self.delfiles_dict = delfiles_dict
            self.delfiles_dict_lock = delfiles_dict_lock
            self.repq = repq

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
                    self.open(self.rootpath + header[1],
                              header[2], header[3])

                elif header[0] == 'release':
                    self.release(header[1], header[2])

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
                buf = f.read()
                f.close()
                l = buf.rsplit(',')
                try:
                    senddata = [0, st, string.atol(l[2])]
                except Exception, e:
                    print "len(l) = %d" % (len(l))
                    print l
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
                buf = f.read()
                f.close()
                l = buf.rsplit(',')
                with self.delfiles_dict_lock:
                    if l[0] in self.delfiles_dict:
                        self.delfiles_dict[l[0]].append(l[1])
                    else:
                        self.delfiles_dict[l[0]] = [l[1], ]
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
                buf = f.read()
                l = buf.rsplit(',')
                buf = "%s,%s,%s" % (l[0], l[1], str(len))
                f.truncate(0)
                f.seek(0)
                f.write(buf)
                f.close()
                senddata = [0, l[0], l[1]]
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
            if os.access(path, os.F_OK) == True:
                """When the required file exist...
                """
                try:
                    DRDFSLog.debug("!!find the file %s w/ %o" % (path, flag))
                    fd = 0
                    if mode:
                        fd = os.open(path, os.O_RDWR, mode[0])
                    else:
                        fd = os.open(path, os.O_RDWR)
                        DRDFSLog.debug("fd = %d" % (fd))
                    buf = os.read(fd, conf.bufsize)
                    l = buf.rsplit(',')
                    dist_l = l[0].rsplit(':')
                    dist = dist_l[0]
                    filename = l[1]
                    size = string.atol(l[2])
                    senddata = [0, dist, fd, size, filename]

                    # add a replication task (now for test)
                    if conf.replication == True:
                        self.repq.put((dist, filename + "test", dist, path))
                        print "task ***"
                except os.error, e:
                    DRDFSLog.debug("!!find the file but error for %s (%s)" % (path, e))
                    senddata = [e.errno, 'null', 'null', 'null', 'null']
                self.c_channel.send_header(senddata)

            else:
                DRDFSLog.debug("can't find the file so create!!")
                try:
                    fd = 0
                    if mode:
                        fd = os.open(path, os.O_RDWR | os.O_CREAT, mode[0])
                    else:
                        fd = os.open(path, os.O_RDWR | os.O_CREAT)
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
                    buf = dist + ',' + filename + ',' + '0'
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
                size = 0
                senddata = [0, dist, fd, size, filename]
                self.c_channel.send_header(senddata)

                
        def release(self, fd, fsize):
            """release handler.

            @param fd file discripter
            @param writelen size of data to be written
            """
            os.lseek(fd, 0, os.SEEK_SET)
            DRDFSLog.debug("fd = " + str(fd))
            try:
                buf = os.read(fd, conf.bufsize)
            except os.error, e:
                print "OSError in release (%s)" %(e)
            l = buf.rsplit(',')

            size = string.atol(l[2])
            if size != fsize:
                try:
                    buf = l[0] + ',' + l[1] + ',' + str(fsize)
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


def main(dir_path):
    meta = DRDFSMeta(dir_path)
    meta.run()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        sys.exit("Usage: ")
    meta = DRDFSMeta(sys.argv[1])
    meta.run()

