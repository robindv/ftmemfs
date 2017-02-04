import re

def is_zookeeper_leader(zk):
    srvr = zk.command('srvr')
    return re.search("Mode: .*?\n", srvr).group().strip()[6:] != "follower"
