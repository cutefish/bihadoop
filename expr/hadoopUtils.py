"""

Hadoop ec2 cluster setup

"""
import os
import re
import shutil
import sys
import getpass
import subprocess

import boto.ec2

userName = getpass.getuser()

def normalizeName(fileName):
    return os.path.abspath(os.path.expanduser(fileName))

def fileToList(fileName):
    f = open(normalizeName(fileName))
    ret = []
    for line in f:
        if line.startswith("#"):
            continue
        ret.append(line.strip())
    return ret

def listToFile(fileName, ls):
    f = open(normalizeName(fileName, 'w'))
    for l in ls:
        f.write(str(l) + '\n')
    f.close()


commandList = ["addConf",
               "master",
               "slaves",
               "cpConf",
               "quickSetup",
               "collectResult",
               "cleanupAll",
              ]

def printUsage():
    print "Available command: ", commandList


def run(argv):
    if len(argv) < 1:
        printUsage()
        sys.exit(-1)
    if (argv[0] == "master"):
        setMaster(argv[1:])
    elif (argv[0] == "slaves"):
        setSlaves(argv[1:])
    elif (argv[0] == 'cpConf'):
        cpConf(argv[1:])
    elif (argv[0] == 'quickSetup'):
        quickSetup(argv[1:])
    elif (argv[0] == 'collectResult'):
        collectResult(argv[1:])
    elif (argv[0] == 'cleanupAll'):
        cleanupAll(argv[1:])
    else:
        printUsage()

"""
Add one key value pair to configuration

Arguments:
    argv[0] --  file
    argv[1] --  key
    argv[2] --  value
"""
def addConf(argv):
    if len(argv) != 3:
        print "hadoop addconf <conf_file> <key> <value>"
        sys.exit(-1)
    confFile = normalizeName(argv[0])
    key = argv[1]
    value = argv[2]
    conf = open(confFile, 'r')
    tmpFile = '/tmp/pyutilTmpConf'
    tmp = open(tmpFile, 'w')
    class State:
        NotFound, Found, Modified = range(3)
    state = State.NotFound
    for line in conf:
        if state == State.Found:
            newLine = re.sub('>.*<', '>' + value + '<', line)
            tmp.write(newLine)
            state = State.Modified
            continue
        if '>' + key + '<' in line:
            state = State.Found
        #end of file
        if '/configuration' in line:
            if state == State.NotFound:
                tmp.write('<property>\n')
                tmp.write('\t<name>%s</name>\n' %key)
                tmp.write('\t<value>%s</value>\n' %value)
                tmp.write('</property>\n\n')
        tmp.write(line)
    conf.close()
    tmp.close()
    shutil.move(tmpFile, confFile)

"""
Set master configurations
"""
def _setMaster(nodeListFile='/tmp/ec2Private',
               hadoopHome='/home/%s/hadoop/hadoop-1.0.3'%userName):
    #read master file
    nodeList = open(nodeListFile)
    master = nodeList.readline().strip()
    #config
    coreSite = '%s/conf/core-site.xml' %hadoopHome
    coreFSKey = 'fs.default.name'
    coreFSValue = 'hdfs://%s:9000' %master
    addConf([coreSite, coreFSKey, coreFSValue])
    mprdSite = '%s/conf/mapred-site.xml' %hadoopHome
    mprdKey = 'mapred.job.tracker'
    mprdValue = 'hdfs://%s:9001' %master
    addConf([mprdSite, mprdKey, mprdValue])
    #change sync master
    tmpFile = '/tmp/hadoopenvtmp'
    envFile = '%s/conf/hadoop-env.sh' %hadoopHome
    tmp = open(tmpFile, 'w')
    env = open(envFile, 'r')
    for line in env:
        if 'HADOOP_MASTER' in line:
            tmp.write('export HADOOP_MASTER=%s:%s\n' %(master, hadoopHome))
        else:
            tmp.write(line)
    tmp.close()
    env.close()
    shutil.move(tmpFile, envFile)

def setMaster(argv):
    if len(argv) == 0:
        _setMaster()
    else:
        if len(argv) != 2:
            print 'hadoop master <nodeListFile> <hadoopHome>'
            sys.exit(-1)
        nodeListFile = normalizeName(argv[0])
        hadoopHome = normalizeName(argv[1])
        _setMaster(nodeListFile, hadoopHome)

"""
Set slaves file
"""
def _setSlaves(numSlaves, nodeListFile='/tmp/ec2Private',
               hadoopHome='/home/%s/hadoop/hadoop-1.0.3'%userName):
    #read slave list
    nodeList = open(nodeListFile)
    nodeList.readline()
    slaves = []
    for line in nodeList:
        slaves.append(line.strip())
    slaves.sort()
    skip = len(slaves) / numSlaves
    #write to file
    slaveFile = open('%s/conf/slaves' %hadoopHome, 'w')
    for i in range(numSlaves):
        slaveFile.write(slaves[i * skip] + '\n')
    slaveFile.close()

def setSlaves(argv):
    if len(argv) == 1:
        _setSlaves(int(argv[0]))
    else:
        if len(argv) != 3:
            print 'hadoop slaves <numSlaves> [nodeListFile hadoopHome]'
            sys.exit(-1)
        numSlaves = int(argv[0])
        nodeListFile = normalizeName(argv[1])
        hadoopHome = normalizeName(argv[2])
        _setSlaves(numSlaves, nodeListFile, hadoopHome)

"""
Copy conf to slaves
"""
def cpConf(argv):
    if len(argv) == 0:
        hadoopHome = '/home/%s/hadoop/hadoop-1.0.3' %userName
    elif len(argv) == 1:
        hadoopHome = argv[0]
    else:
        print "cpConf <hadoopHome>"
        sys.exit(-1)
    slaveList = fileToList('%s/conf/slaves' %hadoopHome)
    for slave in slaveList:
        command = ("scp -i /home/%s/pem/HadoopExpr.pem "
                   "-o UserKnownHostsFile=/dev/null "
                   "-o StrictHostKeyChecking=no "
                   "-r %s/conf %s:%s" %(userName, hadoopHome, slave, hadoopHome))
        print command
        subprocess.call(command.split(' '))

"""
quickSetup
"""
def quickSetup(argv):
    if len(argv) != 1:
        print "quickSetup <numNodes>"
        sys.exit(-1)
    setMaster([])
    setSlaves(argv)
    cpConf([])

"""
parse range
Range is a string representing ranges of integer, using ',' and '-'
e.g. 1,2,3-5 means 1,2,3,4,5
"""
def parseRange(rangeStr):
    commaList = rangeStr.split(',')
    ret = []
    for expr in commaList:
        if '-' not in expr:
            ret.append(int(expr))
        else:
            hyphenList = expr.split('-')
            if len(hyphenList) != 2:
                raise ValueError("range format incorrect: " + expr)
            start = int(hyphenList[0])
            end = int(hyphenList[1])
            for num in range(start, end + 1):
                ret.append(int(num))
    return ret

"""
Collect result from machines
"""
def _collectResult(jobIdPrefix, jobIdRange, dstDir,
                   slaveFile='/home/%s/hadoop/hadoop-1.0.3/conf/slaves'%userName,
                   logHome='/home/%s/hadoop/logs'%userName):
    jobIds = parseRange(jobIdRange)
    slaves = fileToList(slaveFile)
    for jobId in jobIds:
        try:
            os.makedirs('%s/%s_%s' %(dstDir, jobIdPrefix, str(jobId).zfill(4)))
        except OSError:
            pass
        for slave in slaves:
            command = ("scp -i /home/%s/pem/HadoopExpr.pem "
                       "-o UserKnownHostsFile=/dev/null "
                       "-o StrictHostKeyChecking=no "
                       "-r %s:%s/userlogs/%s_%s/* %s/%s_%s"
                       %(userName, slave, logHome, jobIdPrefix, str(jobId).zfill(4),
                         dstDir, jobIdPrefix, str(jobId).zfill(4)))
            print command
            subprocess.call(command.split(' '))

def collectResult(argv):
    if (len(argv) != 3) and (len(argv) != 5):
        print "collectResult <jobIdPrefix> <jobIdRange> <dstDir> [slaveFile, logHome]"
        sys.exit(-1)
    jobIdPrefix = argv[0]
    jobIdRange = argv[1]
    dstDir = argv[2]
    if len(argv) == 3:
        _collectResult(jobIdPrefix, jobIdRange, dstDir)
    else:
        slaveFile = argv[3]
        logHome = argv[4]
        _collectResult(jobIdPrefix, jobIdRange, dstDir, slaveFile, logHome)

"""
clean up
delete /tmp/hadoop*
delete /home/ec2-user/tmp/*
delete /home/hadoop/logs/*
"""
def cleanupAll(argv):
    slaves = fileToList('/home/%s/hadoop/hadoop-1.0.3/conf/slaves' %userName)
    for slave in slaves:
        command = ("ssh -i /home/%s/pem/HadoopExpr.pem "
                   "-o UserKnownHostsFile=/dev/null "
                   "-o StrictHostKeyChecking=no "
                   ' "rm -r /tmp/hadoop*;'
                   ' rm -r /home/%s/tmp/*;'
                   ' rm -r /home/hadoop/logs/*" '
                   %(userName, userName))
        print command
        subprocess.call(command.split(' '));


if __name__ == '__main__':
    run(sys.argv[1:])
