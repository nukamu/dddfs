#! /usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import socket
import select
import string
import cStringIO
import cPickle
import threading
import sys, os
sys.path.append(os.pardir)
import time

from system import DRDFSLog
from conf import conf

class DRDFSChannel(object):
    """This is the class for communication.
    """
    def __init__(self, ):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lock = threading.Lock()

    def set_channel_from_sock(self, sock):
        self.sock = sock

    def connect(self, dist, port):
        self.sock.connect((dist, port))

    def recvall(self, length):
        """recv all data whose length is defined

        This function may return less data than required.
        (when connection closed)
        @param length length of required data
        """
        with self.lock:
            buf = cStringIO.StringIO()
            recvlen = 0
            while recvlen < length:
                recvdata = self.sock.recv(length - recvlen)
                if recvdata == "":
                    self.sock.close()
                    break
                buf.write(recvdata)
                recvlen += len(recvdata)
        data = buf.getvalue()
        return data

    def _recvall(self, length):
        """recv all data whose length is defined

        This function may return less data than required.
        (when connection closed)
        @param length length of required data
        """
        buf = cStringIO.StringIO()
        recvlen = 0
        while recvlen < length:
            recvdata = self.sock.recv(length - recvlen)
            if recvdata == "":
                self.sock.close()
                break
            buf.write(recvdata)
            recvlen += len(recvdata)
        data = buf.getvalue()
        return data

    def send_header(self, senddata):
        with self.lock:
            buf = cPickle.dumps(senddata)
            while conf.bufsize - 3 < len(buf):
                self.sock.sendall(buf[:conf.bufsize - 3] + "---")
                buf = buf[conf.bufsize - 3:]

            buf = buf + "-" * (conf.bufsize - len(buf) - 3) + "end"
            self.sock.sendall(buf)

    def _send_header(self, senddata):
        buf = cPickle.dumps(senddata)
        while conf.bufsize - 3 < len(buf):
            sock.sendall(buf[:conf.bufsize - 3] + "---")
            buf = buf[conf.bufsize - 3:]
            
        buf = buf + "-" * (conf.bufsize - len(buf) - 3) + "end"
        self.sock.sendall(buf)

    def recv_header(self, ):
        with self.lock:
            res_buf = cStringIO.StringIO()
            buf = self._recvall(conf.bufsize)
            if len(buf) != conf.bufsize:
                return None
            res_buf.write(buf[:-3])
            while buf[-3:] != "end":
                buf = self._recvall(conf.bufsize)
                res_buf.write(buf[:-3])
        ret = cPickle.loads(res_buf.getvalue())
        return ret

    def _recv_header(self, ):
        res_buf = cStringIO.StringIO()
        buf = self._recvall(conf.bufsize)
        if len(buf) != conf.bufsize:
            return None
        res_buf.write(buf[:-3])
        while buf[-3:] != "end":
            buf = self._recvall(conf.bufsize)
            res_buf.write(buf[:-3])
        ret = cPickle.loads(res_buf.getvalue())
        return ret

    def send_recv_flow(self, senddata):
        with self.lock:
            self._send_header(senddata)
            ret = self._recv_header()
        return ret

    def brk_channel(self, ):
        self.sock.close()
        self.sock = None
        self.lock = None

if __name__ == '__main__':
    import doctest
    doctest.testmod()
