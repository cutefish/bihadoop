import math
import sys

import pyutils.common.fileutils as fu
import pyutils.common.Configuration as config
import pyutils.expr.run as er

"""
Run Sim Test.

Dependency: PyUtils.
"""
patternDict = {
    'ap'    : ('TestScheduler$AllPair', 'allpair'),
    'hap'   : ('TestScheduler$HalfAllPair', 'halfallpair'),
    'db'    : ('TestScheduler$DiagBlock', 'diagblock'),
    'csb'   : ('TestScheduler$CirShflBlock', 'diagblock'),
    'rsb'   : ('TestScheduler$RandShflBlock', 'diagblock'),
    'rp'    : ('TestScheduler$RandPair', 'randpair'),
    'idb'   : ('TestScheduler$DiagBlock', 'diagblock'),
    'irp'   : ('TestScheduler$RandPair', 'randpair'),
}

def runTest(schedType, reCap, pattern, lenRand, lenConfig, numExprs):
    """Run one sim test.

    Args:
        schedType   --  0:bih, 1:sloc, 2:rand
        reCap       --  if bih, cache capacity, if others, num replication
        pattern     --  ap, hap, db, csb, rsb, rp#xxx, idb, irp#xxx
        lenRand     --  randomness of a record
        lenConfig   --  config string of record length, i0#i1#s0#s1#block
        numExprs    --  number of experiments 
    """
    conf = config.Configuration()
    #default configures
    conf.set("num.distributed.node", 64)
    conf.set("node.block.len", 64)
    conf.set("job.client.num.iterations", 1)
    conf.set("job.split.length.rand", lenRand)
    #customized configuration
    # schedule choice
    conf.set("node.schedule.choice", schedType)
    # cap choice
    if (schedType == 0):
        conf.set('num.replicas', 3)
        conf.set('node.disk.capacity', reCap)
    else:
        conf.set('num.replicas', reCap)
    # pattern p[0]#p[1]
    p = pattern.split('#')
    conf.set('job.class.name', patternDict[p[0]][0])
    if len(p) > 1:
        assert (p[0].endswith('rp')), 'pattern syntax error:%s' %pattern
        conf.set('randpair.dense.level', p[1])
    if (p[0].startswith('i')):
        conf.set('job.client.num.iterations', 10)
    # record length config
    lconf = lenConfig.split('#')
    i0 = i1 = s0 = s1 = b = 1
    if len(lconf) == 5:
        i0, i1, s0, s1, b = lconf
    elif len(lconf) == 4:
        i0, i1, s0, s1 = lconf
    elif len(lconf) == 2:
        i0, s0 = lconf
    else:
        raise Exception('lenConfig incorrect format:%s'%lenConfig)
    conf.set('%s.input0.length' %patternDict[p[0]][1], i0)
    conf.set('%s.input1.length' %patternDict[p[0]][1], i1)
    conf.set('%s.split0.length' %patternDict[p[0]][1], s0)
    conf.set('%s.split1.length' %patternDict[p[0]][1], s1)
    conf.set('%s.num.blocks' %patternDict[p[0]][1], b)

    #write out conf
    conf.write('conf/sim-sched-conf.xml')

    #run test
    rcount, rsum, rave, rstd, rmin, rmax = er.repeatNoneInteract(
        "ant runtest -Dclassname=TestScheduler",
        er.BasicCollector('{k:>>rate}: {v:%float}'), numExprs)
    #print 'count:%s, sum:%s, ave:%s, std:%s, min:%s, max:%s' %(
    #    rcount, rsum, rave, rstd, rmin, rmax)
    print '==>>', 'average rate: ', rave, ' std: ', rstd

def runCompleteTest():
    patternOptions = ['ap', 'hap', 'db', 'csb', 'rsb',
                      'rp#0.2', 'rp#0.4', 'rp#0.8',
                      'idb', 'irp#0.2', 'irp#0.4', 'irp#0.8']
    for p in patternOptions:
        recordLenOptions = [32, 64, 128, 256]
        for l in recordLenOptions:
            lenRandOptions = [0.1, 0.5, 0.9]
            for r in lenRandOptions:
                numTasksOptions = [1, 4, 16]
                for t in numTasksOptions:
                    schedOptions = [0, 1, 2]
                    for s in schedOptions:
                        if (s == 0):
                            capOptions = [64, 256, 1024, 4096]
                        else:
                            capOptions = [3, 8, 16, 32]
                        for c in capOptions:
                            if p.endswith('b'):
                                lenConf = '%s#%s#%s#%s#%s' %(
                                    l * t * 64, l * 64, l, l, 64)
                            else:
                                lenConf = '%s#%s#%s#%s' %(
                                    int(l * math.sqrt(t * 64)),
                                    int(l * math.sqrt(t * 64)),
                                    l, l)
                            print '==>>(%s, %s, %s, %s): %s#%s, %s' %(
                                p, l, r, t, s, c, lenConf)
                            sys.stdout.flush()
                            runTest(s, c, p, r, lenConf, 5)
                            sys.stdout.flush()

def runSingleTest(argv):
    runTest(int(argv[0]), int(argv[1]), argv[2],
            float(argv[3]), argv[4], int(argv[5]))

def main():
    print sys.argv
    if len(sys.argv) == 1:
        runCompleteTest()
    else:
        runSingleTest(sys.argv[1:])

if __name__ == '__main__':
    main()


