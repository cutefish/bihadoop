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
               hadoopHome='/home/%s/hadoop/hadoop-1.0.3'%getpass.getuser()):
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
               hadoopHome='/home/%s/hadoop/hadoop-1.0.3'%getpass.getuser()):
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
        hadoopHome = '/home/%s/hadoop/hadoop-1.0.3' %getpass.getuser()
    elif len(argv) == 1:
        hadoopHome = argv[0]
    else:
        print "cpConf <hadoopHome>"
        sys.exit(-1)
    slaveList = fileToList('%s/conf/slaves' %hadoopHome)
    for slave in slaveList:
        command = "scp -i /home/%s/pem/HadoopExpr.pem -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -r %s/conf %s:%s" %(getpass.getuser(), hadoopHome, slave, hadoopHome)
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

if __name__ == '__main__':
    run(sys.argv[1:])
