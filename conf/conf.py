"""Define ports number used.
There are three ports Mogami uses.
(meta port, data port and prefetch port.)
"""
metaport=15806
dataport=15807

bufsize=1024
blsize=1024 * 1024

writelen_max=1024 * 1024

write_local=True
multithreaded=True

replication=True

rtt_wan=0.027
rtt_lan=0.000017

cluster_conf_file="slaves.conf"
access_db_path="/tmp/accessLoad.sqlite"
