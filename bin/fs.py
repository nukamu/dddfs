#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

from fuse import Fuse
import fuse
fuse.fuse_python_api = (0, 2)
import os, sys, os.path
sys.path.append(os.pardir)

from conf import conf
from libs import channel, dbmng, system, tips
from libs.system import DRDFSLog

import stat, errno, re
import socket, cPickle
import cStringIO, threading
import string, time, select

m_channel = channel.DRDFSChannel()
daemons = []

class DRDFS(Fuse):
    """Class for DRDFS's clients
    """

    def __init__(self, meta_server, *args, **kw):
        Fuse.__init__(self, *args, **kw)
        self.meta_server = meta_server
        self.parse(errex=1)
        m_channel.connect(self.meta_server, conf.metaport)

    def fsinit(self):
        """Called before fs.main() called.
        """
        DRDFSLog.info("** DRDFS FS init **")

        DRDFSLog.debug("Success in creating connection to metadata server")
        DRDFSLog.debug("Init complete!!")
        collector_thread = self.thread_collector(daemons)
        collector_thread.start()
        threads_count = 0

    def finalize(self, ):
        """Finalize of FS
        This seems not to be called implicitly...
        """
        m_channel.brk_channel()
        DRDFSLog.info("** DRDFS Unmount **")

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
                        DRDFSLog.debug("** join thread **")
                        self.daemons.remove(d)
                time.sleep(3)

    """From here functions to register FUSE are written.
    """
    def getattr(self, path):
        DRDFSLog.debug("** getattr ** path = %s" % path)
        senddata = ['getattr', path]

        ans = m_channel.send_recv_flow(senddata)
        if ans[0] != 0:
            return -ans[0]
        else:
            st = tips.DRDFSStat()
            st.load(ans[1])
            if ans[2] >= 0:
                st.chsize(ans[2])
        return st

    def readdir(self, path, offset):
        DRDFSLog.debug("** readdir **" + path + str(offset))
        senddata = ['readdir', path]
        
        ans = m_channel.send_recv_flow(senddata)
        l = ['.', '..']
        if ans[0] == 0:
            l.extend(ans[1])
            return [fuse.Direntry(ent) for ent in l]
        else:
            return -ans[0]

    def unlink(self, path):
        DRDFSLog.debug("** unlink" + path)
        senddata = ['unlink', path]
        
        ans = m_channel.send_recv_flow(senddata)
        return -ans

    def rmdir(self, path):
        DRDFSLog.debug("** rmdir **" + path)
        senddata = ['rmdir', path]

        ans = m_channel.send_recv_flow(senddata)
        return -ans

    def rename(self, path_from, path_to):
        DRDFSLog.debug("** rename **  path_from = %s, path_to = %s" %
                       (path_from, path_to))
        senddata = ['rename', path_from, path_to]
        ans = m_channel.send_recv_flow(senddata)
        return -ans

    def chmod(self, path, mode):
        DRDFSLog.debug("** chmod **")
        return -errno.ENOSYS

    def chown(self, path, user, group):
        DRDFSLog.debug("** chown **")
        return -errno.ENOSYS

    def truncate(self, path, len):
        senddata = ['truncate', path, len]
        DRDFSLog.debug('*** truncate ***' + path + str(len))
        
        ans = m_channel.send_recv_flow(senddata)
        if ans[0] != 0:
            return -ans[0]
        ans[1] = filedist_info_list
        DRDFSLog.debug("send request to data server: path=%s, len=%d" % (path, len)) 
        for filedist_info in filedist_info_list:
            dist = filedist_info[0]
            filename = filedist_info[1]
            senddata = ['truncate', filename, len]
            ch = channel.DRDFSChannel()
            ch.connect(dist, conf.dataport)
            ans = ch.send_recv_flow(senddata)
            ch.brk_channel()
            if ans != 0:
                return -ans
        return 0

    def mknod(self, path, mode, dev):
        DRDFSLog.debug("** mknod **")
        return -errno.ENOSYS

    def mkdir(self, path, mode):
        DRDFSLog.debug("** mkdir **" + path + str(mode))
        senddata = ['mkdir', path, mode]
        
        ans = m_channel.send_recv_flow(senddata)
        return -ans

    def utime(self, path, times):
        DRDFSLog.debug("** utime ** path = %s, times = %s" %
                       (path, str(times)))
        senddata = ['utime', path, times]

        ans = m_channel.send_recv_flow(senddata)
        return -ans

    def access(self, path, mode):
        DRDFSLog.debug("** access **" + path + str(mode))
        senddata = ['access', path, mode]

        ans = m_channel.send_recv_flow(senddata)
        if ans != True:
            return -errno.EACCES
        return 0

    class DRDFSFile(object):
        def __init__(self, path, flags, *mode):
            DRDFSLog.debug("** open ** path = %s, flag = %s, mode = %s" %
                           (path, str(flags), str(mode)))
            senddata = ['open', path, flags, mode]
            
            ans = m_channel.send_recv_flow(senddata)
            DRDFSLog.debug("open ans (from meta)" + str(ans))
            self.dist = ans[1]
            self.metafd = ans[2]
            self.path = path
            self.size = ans[3]
            path_data = ans[4]
            
            DRDFSLog.debug("This file size is %d bytes" % (self.size, ))

            self.d_channel = channel.DRDFSChannel()
            self.d_channel.connect(self.dist, conf.dataport)

            senddata = ['open', path_data, flags, mode]
            ans = self.d_channel.send_recv_flow(senddata)
            DRDFSLog.debug("open ans (from data)" + str(ans))
            if ans[0] != 0:
                print "open error!!"
                return
            self.datafd = ans[1]

            """Initialize the write buffer
            """
            self.r_buflock = threading.Lock()
            self.w_buflock = threading.Lock()
            self.writelist = []
            self.writedata = cStringIO.StringIO()
            self.writelen = 0
        
            self.blnum = self.size / conf.blsize
            if self.size % conf.blsize != 0:
                self.blnum += 1
            if self.size == 0:
                self.bldata = tuple([tips.DRDFSBlock()])
            else:
                self.bldata = tuple([tips.DRDFSBlock() for i in range(self.blnum + 1)])
            DRDFSLog.debug("len of bldata = %d" % (len(self.bldata)))
            DRDFSLog.debug("create bldata 0-%d block" % (len(self.bldata) - 1))

        def read(self, length, offset):
            DRDFSLog.debug("** read ** offset %d, length =%d" % (offset, length))
            requestl = self.cal_bl(offset, length)
            # prepare buffer to return
            ret_str = cStringIO.StringIO()
            DRDFSLog.debug("requestl = %s, with offset: %d, length: %d" %
                            (str(requestl), offset, length))
            for req in requestl:
                reqbl = req[0]
                buf = ""   # buffer for block[reqbl]
                last_readbl = reqbl

                if self.bldata[reqbl].state == 2:
                    buf = self.bldata[reqbl].buf
                elif self.bldata[reqbl].state == 1:
                    DRDFSLog.debug("Waiting recv data %d block" % reqbl)
                    while self.bldata[reqbl].state == 1:
                        time.sleep(0.01)
                    buf = self.bldata[reqbl].buf
                else:
                    ret = self.request_block(reqbl)
                    if ret != 0:
                        DRDFSLog.error("read error! (%d)" % ret)
                        return ret
                    while self.bldata[reqbl].state == 1:
                        time.sleep(0.01)
                    with self.r_buflock:
                        buf = self.bldata[reqbl].buf

                # write strings to return and if reach EOF, break
                ret_str.write(buf[req[1]:req[2]])
                if len(buf) < conf.blsize:
                    break    # end of file

            return ret_str.getvalue()

        def request_block(self, blnum):
            """used when client send a request of block data.
            """
            senddata = ['read', self.datafd, blnum]
            DRDFSLog.debug("** read %d block" % (blnum))
            with self.r_buflock:
                self.bldata[blnum].state = 1
 
            DRDFSLog.debug("request to data server %d block" % (blnum))
            with self.d_channel.lock:
                self.d_channel._send_header(senddata)
                ans = self.d_channel._recv_header()
                if ans == None:
                    return 0
                errno = ans[0]
                blnum = ans[1]
                size = ans[2]
                
                if size == 0:
                    DRDFSLog.debug("*********======")
                    with self.r_buflock:
                        self.bldata[blnum].state = 2
                        self.bldata[blnum].buf = ""
                    return 0
                
                buf = self.d_channel._recvall(size)

            if self.bldata != None:
                with self.r_buflock:
                    self.bldata[blnum].state = 2
                    self.bldata[blnum].buf = buf
            return 0
        
        def cal_bl(self, offset, size):
            """used for calculation of request block number.

            return list which show required block number.
            @param offset offset of read request
            """
            blbegin = offset / conf.blsize
            blend = (offset + size - 1) / conf.blsize + 1
            blnum = range(blbegin, blend)
            
            blfrom = [offset % conf.blsize, ]
            blfrom.extend([0 for i in range(len(blnum) - 1)])
            
            blto = [conf.blsize for i in range(len(blnum) -1)]
            least = (offset + size) % conf.blsize

            if least == 0:
                least = conf.blsize
            blto.append(least)

            return zip(blnum, blfrom, blto)

        def write(self, buf, offset):
            tmp = (offset, len(buf))
            prev_writelen = self.writelen
            with self.w_buflock:
                self.writelist.append(tmp)
                self.writedata.write(buf)
                self.writelen += len(buf)
            if self.writelen > conf.writelen_max:
                self._fflush()
            return len(buf)

        def _fflush(self, ):
            with self.w_buflock:
                buf = cPickle.dumps(self.writelist)
                data = self.writedata.getvalue()
                senddata = ['flush', self.datafd, len(buf), len(data)]
                
                DRDFSLog.debug("flush: fd=%d, listlen=%d, datalen=%d" %
                                (self.datafd, len(buf), len(data)))
                with self.d_channel.lock:
                    self.d_channel._send_header(senddata)
                    self.d_channel.sock.sendall(buf)
                    self.d_channel.sock.sendall(data)
                    ans = self.d_channel._recv_header()
                self.writelen = 0
                self.writelist = []
                self.writedata = cStringIO.StringIO()
                self.dirty_dict = {}
            return ans

        def flush(self):
            if self.writelen == 0:
                pass
            else:
                self._fflush()
            return 0

        def release(self, flags):
            DRDFSLog.debug('** release')
            if self.writelen == 0:
                pass
            else:
                self._fflush()

            senddata = ['release', self.datafd]
            ans = self.d_channel.send_recv_flow(senddata)
            self.bldata = None
            self.timedic = None

            senddata = ['release', self.metafd, ans]
            
            ans = m_channel.send_recv_flow(senddata)
            return 0

#        def fgetattr(self):
#            return 0

#        def ftruncate(self, len):
#            return 0
        

    def main(self, *a, **kw):
        self.file_class = self.DRDFSFile
        return Fuse.main(self, *a, **kw)


if __name__ == '__main__':
    DRDFSLog.init("fs", DRDFSLog.DEBUG)
    fs = DRDFS(sys.argv[1],
               version="%prog " + fuse.__version__,
               usage=system.usagestr(), )
    fs.multithreaded = conf.multithreaded
    fs.main()
    fs.finalize()
