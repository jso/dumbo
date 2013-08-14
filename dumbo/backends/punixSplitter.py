import os, sys

# assume reducer directory has been created

mapId = int(sys.argv[1])
nReducers = int(sys.argv[2])
outdir = sys.argv[3]

files = {}

from IPy import IP
import datetime

for line in sys.stdin:
    key, val = line.split("\t", 1)
    outnum = hash(eval(key)) % nReducers
    if outnum not in files:
        f = open(os.sep.join([outdir, "r-%d" % outnum, "m-%d" % mapId]), "w")
        files[outnum] = f
    files[outnum].write(line)

for i in files:
    files[i].close()

