'''
Created on 8 Jul 2011

@author: jso
'''

import sys, os, time, threading
import operator

from dumbo.backends.common import Backend, Iteration, FileSystem
from dumbo.util import getopt, getopts, configopts, envdef, execute
from dumbo.cmd import decodepipe

from multiprocessing import Pool

from dumbo.backends.unix import UnixFileSystem

import subprocess

import Queue
import random

master_debug = False

class PunixBackend(Backend):
    
    def matches(self, opts):
        return bool(getopt(opts, 'punix', delete=False))
        
    def create_iteration(self, opts):
        progopt = getopt(opts, 'prog')
        return PunixIteration(progopt[0], opts)

    def create_filesystem(self, opts):
        # are we given a specific shell?
        shell = getopt(opts, "shell", delete=False)
        if shell:
            return UnixFileSystem(shell[0])
        else:
            return UnixFileSystem()

def doMap(*args, **kwargs):
    f = None
    retval = 1
    try:
        pyenv, python, cmdenv, mapper, nReducers, tmpdir, output, addedopts, shell, mapi, filename_list, doReduces = args

        filenames = " ".join(["-file %s" % x for x in filename_list])

        encodepipe = pyenv + ' ' + python + ' -m dumbo.cmd encodepipe ' + filenames
        if addedopts['inputformat'] and addedopts['inputformat'][0] == 'code':
            encodepipe += ' -alreadycoded yes'

        if doReduces:
            cmd = "%s | %s %s %s | python -m dumbo.backends.punixSplitter %d %d %s" % (encodepipe,
                                              pyenv,
                                              cmdenv,
                                              mapper,
                                              mapi,
                                              nReducers,
                                              tmpdir)

        else:
            outfile = os.sep.join([output, "part-%05d" % mapi])
            cmd = "%s | %s %s %s > '%s'" % (encodepipe,
                                              pyenv,
                                              cmdenv,
                                              mapper,
                                              outfile)
            

        cmdStderr = open(os.sep.join([tmpdir, "m-%d-status" % mapi]), "w")
        retval = execute(cmd, stderr=cmdStderr, executable=shell)
        cmdStderr.close()


        f = open(os.sep.join([tmpdir, "m-%d-status" % mapi]), "a")
        print >>f, "return code:", retval
        f.close()

        if retval != 0:
            print "map %d failed" % mapi

    except Exception as e:
        f = open(os.sep.join([tmpdir, "m-%d-status" % mapi]), "a")
        print >>f, type(e), str(e)
        f.close()

    return retval

def doReduce(*args, **kwargs):
    retval = 1
    try:
        tmpdir, pyenv, cmdenv, reducer, output, shell, reducenum = args

        combinedInput = os.sep.join([tmpdir, "r-%d-all" % reducenum])

        retval = 0
        if os.path.exists(combinedInput):
            cmd = "LC_ALL=C sort -t $'\\t' --temporary-directory=%s --key=1 %s | %s %s %s > '%s'" % (tmpdir, combinedInput, pyenv, cmdenv, reducer, os.sep.join([output, "part-%05d" % reducenum]))

            cmdStderr = open(os.sep.join([tmpdir, "r-%d-status" % reducenum]), "w")
            retval = execute(cmd, stderr=cmdStderr, executable=shell)
            cmdStderr.close()

            f = open(os.sep.join([tmpdir, "r-%d-status" % reducenum]), "a")
            print >>f, "return code:", retval
            f.close()

            if not master_debug:
                # clean up
                os.remove(combinedInput)

    except Exception as e:
        f = open(os.sep.join([tmpdir, "r-%d-status" % reducenum]), "a")
        print >>f, type(e), str(e)
        f.close()

    if retval != 0:
        print "reduce %d failed" % reducenum

    return retval

class MapErrOutputCopier(threading.Thread):
    def __init__(self, tmpdir):
        threading.Thread.__init__(self)

        self.tmpdir = tmpdir
        self.dstFile = "%s.mapStatus.txt" % self.tmpdir

        self.q = Queue.Queue()

    def run(self):
        f = open(self.dstFile, "w")
        while True:
            mapi = self.q.get()
            if mapi is None: break

            # copy the file to dstFile and remove the original
            toCopy = os.sep.join([self.tmpdir, "m-%d-status" % mapi])
            if os.path.exists(toCopy):
                print >>f, "map %d" % mapi
                g = open(toCopy)
                for l in g:
                    if "WARNING: skipping bad value" in l: continue
                    if "reporter:counter:Dumbo" in l: continue
                    f.write(l)
                g.close()
                os.remove(toCopy)
        f.close()

    def map_done(self, mapi):
        self.q.put(mapi)

class ReduceErrOutputCopier(threading.Thread):
    def __init__(self, tmpdir):
        threading.Thread.__init__(self)

        self.tmpdir = tmpdir
        self.dstFile = "%s.reduceStatus.txt" % self.tmpdir

        self.q = Queue.Queue()

    def run(self):
        f = open(self.dstFile, "w")
        while True:
            reducenum = self.q.get()
            if reducenum is None: break

            # copy the file to dstFile and remove the original
            toCopy = os.sep.join([self.tmpdir, "r-%d-status" % reducenum])
            if os.path.exists(toCopy):
                print >>f, "reduce %d" % reducenum
                g = open(toCopy)
                for l in g:
                    f.write(l)
                g.close()
                os.remove(toCopy)
        f.close()

    def reduce_done(self, reducenum):
        self.q.put(reducenum)

class ReduceInputCopier(threading.Thread):
    def __init__(self, tmpdir, rnum):
        threading.Thread.__init__(self)

        self.tmpdir = tmpdir
        self.rnum = rnum
        self.q = Queue.Queue()

        self.dstFile = os.sep.join([tmpdir, "r-%d-all" % rnum])

    def run(self):
        while True:
            mapi = self.q.get()
            if mapi is None: break

            # copy the file to dstFile and remove the original
            toCopy = os.sep.join([self.tmpdir, "r-%d" % self.rnum, "m-%d" % mapi])
            if os.path.exists(toCopy):
                subprocess.call("cat %s >> %s" % (toCopy, self.dstFile), shell=True, executable="/bin/bash")
                os.remove(toCopy)

        os.rmdir(os.sep.join([self.tmpdir, "r-%d" % self.rnum]))

    def map_done(self, mapi):
        self.q.put(mapi)

class PunixIteration(Iteration):

    def __init__(self, prog, opts):
        Iteration.__init__(self, prog, opts)
        self.opts += configopts('punix', prog, self.opts)

    def run(self):
        retval = Iteration.run(self)
        if retval != 0:
            return retval
        addedopts = getopts(self.opts, ['input',
                                        'output',
                                        'mapper',
                                        'reducer',
                                        'libegg',
                                        'delinputs',
                                        'cmdenv',
                                        'inputformat',
                                        'outputformat',
                                        'numreducetasks',
                                        'python',
                                        'pypath',
                                        'tmpdir',
                                        'nmappers',
                                        'nreducers',
                                        'permapper',
                                        'shell'])
        (mapper, reducer) = (addedopts['mapper'][0], addedopts['reducer'][0])
        if not addedopts['input'] or not addedopts['output']:
            print >> sys.stderr, 'ERROR: input or output not specified'
            return 1
        inputs = reduce(operator.concat, (input.split(' ') for input in
                        addedopts['input']))
        output = addedopts['output'][0]
        try: os.makedirs(output)
        except os.error as e: pass

        pyenv = envdef('PYTHONPATH', addedopts['libegg'],
                       shortcuts=dict(configopts('eggs', self.prog)),
                       extrapaths=addedopts['pypath'])
        cmdenv = ' '.join("%s='%s'" % tuple(arg.split('=')) for arg in
                          addedopts['cmdenv'])

        shell = addedopts["shell"][0]

        python = addedopts['python'][0]

        mapTotal = len(inputs)
        mapDoneCount = [0]
        reduceDoneCount = [0]

        nMappers = int(addedopts["nmappers"][0])
        nReducers = int(addedopts["nreducers"][0])

        # this is the number of files that will be handed to each mapper
        permapper = int(addedopts["permapper"][0])

        # start the mappers, reducers
        mPool = Pool(nMappers)
        rPool = Pool(nReducers)

        doReduces = not (addedopts['numreducetasks'] and addedopts['numreducetasks'][0] == '0')

        # set up the mapper output/reducer input directories
        tmpdir = os.sep.join([addedopts['tmpdir'][0], "%s_%06d" % (time.strftime("%Y-%m-%d_%H-%M-%S", time.gmtime()), random.randint(0, 999999))])

        mLock = threading.Lock()
        mResults = {}
        rLock = threading.Lock()
        mByR = {}
        rStarted = set()
        rResults = {}

        # start the map status output copier
        mapErrOutputCopier = MapErrOutputCopier(tmpdir)
        mapErrOutputCopier.start()

        if doReduces:
            # start the copy threads to handle map outputs
            copyLock = threading.Lock()
            copyThreads = {}
            for i in range(nReducers):
                copyThreads[i] = ReduceInputCopier(tmpdir, i)
                copyThreads[i].start()

            # start the reduce status output copier
            reduceErrOutputCopier = ReduceErrOutputCopier(tmpdir)
            reduceErrOutputCopier.start()

            for i in range(nReducers):
                try: os.makedirs(os.sep.join([tmpdir, "r-%d" % i]))
                except os.error as e: pass

                mByR[i] = set()

        # do maps -- kick it all off
        if permapper == 1:
            for args in enumerate(inputs):
                i, filename = args
                args = pyenv, python, cmdenv, mapper, nReducers, tmpdir, output, addedopts, shell, i, [filename], doReduces
                mLock.acquire()
                mResults[i] = mPool.apply_async(doMap, args)
                mLock.release()
        else:
            # multiple files per mapper...
            remaining = list(inputs)
            i = 0
            while remaining:
                args = pyenv, python, cmdenv, mapper, nReducers, tmpdir, output, addedopts, shell, i, remaining[:permapper], doReduces
                mLock.acquire()
                mResults[i] = mPool.apply_async(doMap, args)
                mLock.release()

                remaining = remaining[permapper:]
                i += 1
            
            # need to reset the mapTotal variable since we have fewer tasks...
            mapTotal = i

        def reduceDone():
            # did anything finish?
            rLock.acquire()

            done = [x for x in rResults if rResults[x].ready()]
            for args in done: del rResults[args] # cleanup

            rLock.release()

            for reducenum in done:
                #print "reduce %d done" % reducenum
                reduceDoneCount[0] += 1

                reduceErrOutputCopier.reduce_done(reducenum)

        def mapDone():
            # did anything finish?
            mLock.acquire()

            done = [x for x in mResults if mResults[x].ready()]
            for args in done: del mResults[args] # cleanup

            mLock.release()

            for args in done:
                i = args

                mapDoneCount[0] += 1

                mapErrOutputCopier.map_done(i)

                if doReduces:
                    #print "map %d done" % i

                    # update the structures
                    for reducenum in range(nReducers):
                        # initiate the copy request...
                        copyThreads[reducenum].map_done(i)

                        rLock.acquire()

                        mByR[reducenum].add(i)

                        # see if we can signal that's all the copier will have to handle?
                        if len(mByR[reducenum]) == mapTotal:
                            copyThreads[reducenum].map_done(None)

                        rLock.release()
                else:
                    # just move the map output file (unsorted) to the output directory
                    print "map %d done" % i


        def copyDone():
            # did anything finish?
            copyLock.acquire()

            done = [x for x in copyThreads if not copyThreads[x].is_alive()]

            for rnum in done: del copyThreads[rnum] # cleanup

            copyLock.release()

            for rnum in done:
                rLock.acquire()

                rStarted.add(rnum)
                args = tmpdir, pyenv, cmdenv, reducer, output, shell, rnum
                rResults[rnum] = rPool.apply_async(doReduce, args)

                rLock.release()

        while reduceDoneCount[0] < nReducers:
            # check for things finishing...
            mapDone()
            copyDone()
            reduceDone()

            mLock.acquire()
            haveMaps = len(mResults)
            mLock.release()

            rLock.acquire()
            haveReduces = len(rResults)
            rLock.release()

            copyLock.acquire()
            copyRunning = len(copyThreads)
            copyLock.release()

            print "%d/%d/%d maps\t%d/%d copies\t%d/%d/%d reduces" % (haveMaps, mapDoneCount[0], mapTotal, copyRunning, nReducers, haveReduces, reduceDoneCount[0], nReducers)

            time.sleep(5)

        mPool.terminate()
        mPool.join()
        rPool.terminate()
        rPool.join()

        # make sure the map status output is done before cleaning up the tmp dir
        mapErrOutputCopier.map_done(None)
        mapErrOutputCopier.join()

        if doReduces:
            # make sure the reduce status output is done before cleaning up the tmp dir
            reduceErrOutputCopier.reduce_done(None)
            reduceErrOutputCopier.join()

        if not master_debug and len(os.listdir(tmpdir)) == 0:
            os.rmdir(tmpdir)


        return 0 # make sure we return an error if there is a problem.

