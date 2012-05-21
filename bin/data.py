#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, os.path
sys.path.append(os.pardir)

from libs import channel
from libs.system import DRDFSLog
from conf import conf

import stat, errno, re, string
import cPickle, socket
import time
import cStringIO

import threading
import random
import atexit     # for leave from the list of data servers

class DRDFSData(object):
    """This is the class for DRDFS's data servers
    """
    def __init__(self, metaaddr, rootpath):
        """initialize DRDFS's data server daemon
        """
        self.metaaddr = metaaddr
        self.rootpath = os.path.abspath(rootpath)

        """Check directory for data files.
        """
        assert os.access(self.rootpath, os.R_OK and os.W_OK and os.X_OK)
        
        """Initialize Log
        """
        DRDFSLog.init("data", DRDFSLog.DEBUG)

        """At first, connect to metadata server and send request to attend.
        """
        mchannel = channel.DRDFSChannel()
        mchannel.connect(self.metaaddr, conf.metaport)

        DRDFSLog.debug("Success in creating connection to metadata server")
        senddata = ['dataadd', self.rootpath]
        
        mchannel.send_header(senddata)
        mchannel.brk_channel()

        DRDFSLog.debug("Init complete!!")        

    def run(self, ):
        """Connected from clients
        """
        self.lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.lsock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lsock.bind(("0.0.0.0", conf.dataport))
        self.lsock.listen(10)
        
        daemons = []
        collector_thread = self.thread_collector(daemons)
        collector_thread.start()
        threads_count = 0
        
        while True:
            (csock, address) = self.lsock.accept()
            c_channel = channel.DRDFSChannel()
            c_channel.set_channel_from_sock(csock)
            datad = self.handler(c_channel, self.rootpath)
            datad.name = "D%d" % (threads_count)
            threads_count += 1
            datad.start()
            daemons.append(datad)

    def finalize(self, ):
        """remove myself from data servers' list
        """
        mchannel = channel.DRDFSChannel()
        mchannel.connect(self.metaaddr, conf.metaport)
        senddata = ['datadel']

        mchannel.send_header(senddata)
        mchannel.brk_channel()

    class thread_collector(threading.Thread):
        """Collect dead threads in arguments.
        
        @param threads thread list to collect: alive and dead threads are included
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
                time.sleep(1)

    class handler(threading.Thread):
        """class for the thread created for each client
        This handler is run as multithreads
        """
        def __init__(self, ch, rootpath):
            threading.Thread.__init__(self)
            self.setDaemon(True)
            self.c_channel = ch
            self.rootpath = rootpath

        def run(self, ):
            while True:
                header = self.c_channel.recv_header()

                if header == None:
                    DRDFSLog.debug("Connection closed")
                    break

                if header[0] == 'open':
                    DRDFSLog.debug('** open')
                    self.open(header[1], header[2], *header[3])
                
                elif header[0] == 'create':
                    DRDFSLog.debug('** create')
                    self.create(header[1], header[2], header[3])

                elif header[0] == 'read':
                    DRDFSLog.debug('** read')
                    self.read(header[1], header[2])
                
                elif header[0] == 'flush':
                    DRDFSLog.debug('** flush')
                    self.flush(header[1], header[2], header[3])

                elif header[0] == 'release':
                    DRDFSLog.debug('** release')
                    self.release(header[1])
                    break
                
                elif header[0] == 'truncate':
                    DRDFSLog.debug('** truncate')
                    self.truncate(header[1], header[2])
                
                elif header[0] == 'ftruncate':
                    DRDFSLog.debug('** ftruncate')
                    
                elif header[0] == 'close':
                    DRDFSLog.debug('** close')
                    self.c_channel.brk_channel()
                    break

                elif header[0] == 'filedel':
                    DRDFSLog.debug('** filedel')
                    self.filedel(header[1])

                elif header[0] == 'do_repl':
                    DRDFSLog.debug('** do replication **')
                    """header[1]: file path to replicate
                    header[2]: IP address of the node where new replica will be located
                    """
                    self.replication(header[1], header[2])

                elif header[0] == 'recv_repl':
                    DRDFSLog.debug('** recv replication **')
                    """header[1]: file path to replicate
                    header[2]: file replica size
                    """
                    self.recv_replication(header[1], header[2])
                    
                else:
                    DRDFSLog.debug('** this is unexpected header. break!')
                    break


        def open(self, path, flag, *mode):
            DRDFSLog.debug("path = %s, flag = %s, mode = %s" %
                           (path, str(flag), str(mode)))
            """I should fill in this function.
            Flow of creating new file should be written.
            """
            flag = flag & ~os.O_EXCL

            try:
                fd = os.open(os.path.join(self.rootpath, path),
                             flag, *mode)
                senddata = [0, fd]
            except Exception, e:
                senddata = [e.errno, 0]
            self.c_channel.send_header(senddata)

        def read(self, fd, bl_num):
            DRDFSLog.debug("fd = %d, bl_num = %d" % (fd, bl_num))
            try:
                os.lseek(fd, bl_num * conf.blsize, os.SEEK_SET)

                buf = cStringIO.StringIO()
                readlen = 0
                while readlen < conf.blsize - 1:
                    os.lseek(fd, bl_num * conf.blsize + readlen, os.SEEK_SET)
                    tmpbuf = os.read(fd, conf.blsize - readlen)
                    if tmpbuf == '':   # end of file
                        break
                    buf.write(tmpbuf)
                    readlen += len(tmpbuf)
                sendbuf = buf.getvalue()
                DRDFSLog.debug("read from file offset %d len %d (result %d)" % 
                               (bl_num * conf.blsize, conf.blsize, len(sendbuf)))
                senddata = [0, bl_num, len(sendbuf)]
            except Exception, e:
                DRDFSLog.error("read have an error (%s)" % (e))
                senddata = [e.errno, 'null', 0, 0]
            self.c_channel._send_header(senddata)
            self.c_channel.sock.sendall(sendbuf)
            

        def flush(self, fd, listlen, datalen):
            DRDFSLog.debug("flush: fd=%d, listlen=%d, datalen=%d" % (fd, listlen, datalen))

            buf = self.c_channel._recvall(listlen)
            writelist = cPickle.loads(buf)
            buf = self.c_channel._recvall(datalen)

            write = 0
            for wd in writelist:
                try:
                    os.lseek(fd, wd[0], os.SEEK_SET)
                    res = os.write(fd, buf[write:write+wd[1]])
                    write += res
                    if res != wd[1]:
                        DRDFSLog.error("write length error !!")
                    DRDFSLog.debug("write from offset %d (result %d)" % (wd[0], res))
                except Exception, e:
                    senddata = -e.errno

            senddata = write
            self.c_channel._send_header(senddata)
                
        def release(self, fd):            
            os.fsync(fd)
            st = os.fstat(fd)
            os.close(fd)
            senddata = st.st_size
            self.c_channel.send_header(senddata)

        # System APIs
        def filedel(self, file_list):
            for file_name in file_list:
                os.unlink(os.path.join(self.rootpath, file_name))
            senddata = 0
            self.c_channel.send_header(senddata)

        # for replication (required by metadata server)
        def replication(self, filename, to_addr):
            # for test
            filename = filename[:-4]

            to_channel = channel.DRDFSChannel()
            to_channel.connect(to_addr, conf.dataport)

            f = open(os.path.join(self.rootpath, filename), 'r')
            f_size = os.fstat(f.fileno()).st_size
            filename = filename + 'test'
            senddata = ['recv_repl', filename, f_size]
            to_channel._send_header(senddata)
            send_size = 0

            while send_size < f_size:
                buf = f.read(conf.blsize)
                to_channel.sock.sendall(buf)
                send_size += len(buf)
            assert (send_size == f_size)

            ans = 0
            self.c_channel.send_header(ans)
            to_channel.brk_channel()

        # for replication (required by a data server)
        def recv_replication(self, filename, f_size):
            """
            @param filename filename of replica data
            @param f_size size of the original file
            """
            f = open(os.path.join(self.rootpath, filename), 'w+')
            
            write_size = 0
            while write_size < f_size:
                buf = self.c_channel._recvall(conf.blsize)
                f.write(buf)
                write_size += len(buf)            
            f.close()
            sys.exit()

def main(meta_addr, dir_path):
    data = DRDFSData(meta_addr, dir_path)
    atexit.register(data.finalize)
    data.run()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit("Usage: ")
    data = DRDFSData(sys.argv[1], sys.argv[2])
    atexit.register(data.finalize)
    data.run()
