#! /usr/bin/env python
# -*- coding: utf-8 -*-

from optparse import OptionParser
import fs
import server

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage)
    parser.add_option("-s", "--server", action="store_true",
                      dest="mode")
    parser.add_option("-c", "--client", action="store_false",
                      dest="mode")
    (options, args) = parser.parse_args()
#    if len(args) != 1:
#        parser.error("args are not defined")

if __name__ == '__main__':
    main()
