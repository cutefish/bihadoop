import sys

import pyutils.common.fileutils as fu
import pyutils.common.Configuration as config
import pyutils.expr.run as er

"""
Run Sim Test.

Dependency: PyUtils.
"""

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
    conf.set("job.split.length.rand", 0.1)
    #customized configuration
    # schedule choice
    conf.set("node.schedule.choice", schedType)
    # cap choice
    if (schedType == 0):
        conf.set('node.replicas', 3)
        conf.set('node.disk.capacity', reCap)
    else:
        conf.set('node.replicas', reCap)
    # pattern p[0]#p[1]
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
        er.BasicCollector('{k:>>rate}: {v:%float}'), 5)
    print 'count:%s, sum:%s, ave:%s, std:%s, min:%s, max:%s' %(
        rcount, rsum, rave, rstd, rmin, rmax)

def main():
    runTest(1, 1024, 'ap', 0.1, '4096#4096#128#128', 5)

if __name__ == '__main__':
    main()


