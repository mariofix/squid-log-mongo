#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo, sys, os, time

#
# based on script written by Matt Franz
# http://blogfranz.blogspot.com/2010/01/dummies-guide-to-mongodb-queries-using.html
#

logfile = "/var/log/squid3/access.log"
m=('127.0.0.1','squid2mongo')


def has_entry(collection,stamp):
    return False

def parse_line(collection,l, macs):
    tags = {"source":2,"squidcode":3,"size":4,"method":5,"url":6,"format":7,"mime":9}

    f = l.split()    
    stamp = float(f[0])
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(stamp)))
    if not has_entry(coll,stamp):
        d = {}
        d['stamp'] = stamp
        d['timestamp'] =  timestamp
        for field  in tags.keys():
            if field in ['size']:
                d[field] = float(f[tags[field]])
            elif field in ['url']:
                d[field] = f[tags[field]].decode('unicode_escape').encode('ascii','ignore')
            elif field in ['source']:
                d[field] = f[tags[field]]
                d['mac'] = "S/I"
                for m in macs:
                    if m['ip'] == f[tags[field]]:
                        d['mac'] = m['mac']
                        break
            elif field in ['mime']:
                d[field] = f[tags[field]].decode('unicode_escape').encode('ascii','ignore')
            else:
                d[field] = f[tags[field]]
        return d
    else:
        return {}

# from http://stackoverflow.com/questions/136168/get-last-n-lines-of-a-file-with-python-similar-to-tail
def tail( f, window=2048 ):
    f.seek( 0, 2 )
    bytes= f.tell()
    size= window
    block= -1

    while size > 0 and bytes+block*1024  > 0:
        f.seek( block*2048, 2 ) # from the end!
        data= f.read( 2048 )
        linesFound= data.count('\n')
        size -= linesFound
        block -= 1

    f.seek( block*2048, 2 )
    f.readline() # find a newline
    lastBlocks= list( f.readlines() )
    return lastBlocks



###

if __name__ == "__main__":

    conn = pymongo.Connection(m[0])
    db = conn[m[1]]
    coll = db["raw"]

    c = os.popen("arp | grep ether")
    macs = c.read().splitlines()
    lista_macs = []
    for m in macs:
        l = m.split()
        lista_macs.append({"ip":l[0], "mac": l[2]})
    
    #print lista_macs
    
    #exit(0)
    if sys.argv[1] == "tail":
        lines = tail(open(logfile,"r"))
        for l in lines:
            d = parse_line(coll, l, lista_macs)
            if d != {}:
                try:
                    coll.save(d)
                except:
                    print d
    elif sys.argv[1] == "all":
        f = open(logfile)
        for l in f:
            linea = parse_line(coll,l, lista_macs)
            try:
                coll.save(linea)
            except:
                print linea
            #print parse_line(coll, l, lista_macs)
    else:
        f = open(sys.argv[1])
        for l in f:
            coll.save(parse_line(coll,l))
