from conf import CASSANDRA_HOME, YCSB_HOME, CASS_NODES
import commands
import subprocess
from time import sleep, time
import sys
from getopt import getopt
from argparse import ArgumentParser
import readline

nodes = {}

def execute(cmd):
    return commands.getstatusoutput(cmd)

def sed(file, find, repl):
    execute('sed -i -e \'s/%s/%s/g\' %s' % (escape(find), escape(repl), file))

def escape(path):
    path = path.replace('#', '\#')
    return path.replace('/', '\/')

def init_nodes():
    execute('rm -rf ' + CASS_NODES)

    seeds = ','.join( ['127.0.0.' + str(i+1) for i in range(NUM_START, NUM_NODES+1) ] )
    print 'Seeds:', seeds

    for i in range(0, NUM_NODES+1):
        token = 2**127 / NUM_NODES * (i-1) if i != 0 else 1
        listenAddr = '127.0.0.' + str(i+1)
        jmxPort = str(7199 + i - 1)

        nodeFolder = 'node' + str(i)
        absNodeFolder = CASS_NODES + '/' + nodeFolder
        execute('mkdir -p ' + absNodeFolder)

        # Which files should we perform replacements?
        config_files = ['start_cass.sh', 
                        'cassandra.in.sh',
                        'conf/cassandra.yaml', 
                        'conf/cassandra-env.sh',
                        'conf/log4j-server.properties']

        # Replace the #KEY# with the value.
        substitutions = {'INITIAL_TOKEN': token,
                         'CASSANDRA_HOME': CASSANDRA_HOME,
                         'NODE_FOLDER': absNodeFolder,
                         'NODE_IP': listenAddr,
                         'SEED_LIST': seeds,
                         'JMX_PORT': jmxPort,
                        }

        # Copy the default config files over.
        execute('cp -r conf %s/' % absNodeFolder)
        execute('cp start_cass.sh %s' % absNodeFolder)
        execute('cp cassandra.in.sh %s' % absNodeFolder)


        # Perform the variable replacement for all files.
        for f in config_files:
            for (var, subst) in substitutions.items():
                sed(absNodeFolder + '/' + f, '#' + var + '#', str(subst))

def create_keyspace():
    execute('sh %s/bin/cassandra-cli -h 127.0.0.2 -f %s/testing/create_keyspace.sql' % (CASSANDRA_HOME, CASSANDRA_HOME))

def start_nodes():
    for i in range(NUM_START, NUM_NODES+1):
        start_node(i)

def start_node(i):
    nodeFolder = 'node' + str(i)
    absNodeFolder = CASS_NODES + '/' + nodeFolder
    execute('mkdir %s' % absNodeFolder)
    logFile = open('%s/log.txt' % absNodeFolder, 'w')
    errFile = open('%s/err.txt' % absNodeFolder, 'w')
    proc = subprocess.Popen('bash %s/start_cass.sh' % absNodeFolder, shell=True, stdout=logFile, stderr=errFile)
    nodes[i] = proc
    print 'Started %s as pid=%s' % (nodeFolder, proc.pid)
    logFile.close()
    errFile.close()

def check_nodes():
    for i in range(NUM_START, NUM_NODES+1):
        nodeFolder = 'node' + str(i)
        absNodeFolder = CASS_NODES + '/' + nodeFolder
        errFile = open('%s/err.txt' % absNodeFolder, 'r')
        errs = errFile.read()
        if len(errs) > 0:
            print
            print
            print '=== Problem: %s has an error, listed below ===' % nodeFolder
            print errs
            if not args.ignoreerrors:
                sys.exit(-1)
            else:
                print
                print 'This is probably not OK, but you told me to ignore errors.'
                print '-----'
        errFile.close()


def wait():
    numTicks = 80
    for i in range(numTicks + 1):
        print '[' + 'X'*i + ' '*(numTicks-i) + ']',
        sys.stdout.flush()
        check_nodes()
        sleep(float(args.w)/numTicks)
        print '\b' * (numTicks+4),
        sys.stdout.flush()
    print


parser = ArgumentParser(description='Starts some Cass servers!')
parser.add_argument('-n', help='Number of nodes. Default 3.', default=3)
parser.add_argument('-w', help='How long to wait for nodes to come alive. Default 20s.', default=20)
parser.add_argument('-coord', help='Start up the coordinator node (node0)', action='store_const', const=True)
parser.add_argument('-noinit', help='Do not re-initialize nodes', action='store_const', const=True)
parser.add_argument('-nostart', help='Do not start the nodes', action='store_const', const=True)
parser.add_argument('-nokilljava', help='Do not kill java (to stop previous nodes)', action='store_const', const=True)
parser.add_argument('-ignoreerrors', help='Ignore server start errors', action='store_const', const=True)
args = parser.parse_args()

NUM_START = 1 if not args.coord else 0
NUM_NODES = int(args.n)

print 'Happy days!'
if not args.nokilljava:
    execute('killall java')

if not args.noinit:
    init_nodes()
    print 'Configuration of %s nodes%s complete, starting shortly...' % (NUM_NODES, ' (+ 1 coordinator)' if args.coord else '')
    sleep(2)

if not args.nostart:
    print 'Starting nodes...'
    start_nodes()

    print 'Waiting %s seconds for nodes to finish startup...' % args.w
    wait()

    # Print a god damn loading bar for the fucking hell of it.

if not args.noinit:
    print 'Creating keyspace `usertable`.'
    create_keyspace()

def extractNodeFromCmd(cmds):
    if cmds[1][0:4] == 'node':
        node = int(cmds[1][4:].strip())
    else:
        node = int(cmds[1])
    return node

shell_commands = ['help', 'quit', 'kill', 'start', 'restart', 'status', 'ring', 'wait', 'run']

def completer(text, state):
    skipNext = state
    for command in shell_commands:
        if command.startswith(text):
            if skipNext == 0:
                return command
            else:
                skipNext -= 1
#    return text

readline.set_completer(completer)
readline.parse_and_bind("tab: complete")

if not args.nostart:
    print 'Ready! Now accepting commands.'
    while True:
        inp = raw_input('> ').lower()
        cmds = inp.split(' ')
        
        if cmds[0] == 'h' or cmds[0] == 'help':
            print 'Like I\'d help you.'
            print 'Commands:'
            print '\tq, quit'
            print '\tr, run TC - Runs ycsb with the given threadcount'
            print '\tk, kill - Kills a particular node. E.g., `kill 2`'
            print '\ts, start - Starts a particular node. E.g., `start 2`'
            print '\trestart - Kills and starts a node some # of seconds later. E.g., `restart 2 15`'
            print '\tring, status - Run nodetool ring or status.'
            print '\twait - Waits for the standard time for a node to come alive.'

        elif cmds[0] == 'q' or cmds[0] == 'quit' or cmds[0] == 'exit':
            print 'Good-bye!'
            execute('killall java')
            sys.exit(0)

        elif cmds[0] == 'k' or cmds[0] == 'kill':
            try:
                node = extractNodeFromCmd(cmds)
                assert 1 <= node <= NUM_NODES  # Why would you want to kill the coordinator?
                
                (status, output) = execute('ps -C java ww')
                procs = output.splitlines()
                procId = None
                for proc in procs:
                    if 'node%s/conf' % node in proc:
                        procId = int(proc.strip().split(' ')[0])
            
                if procId == None:
                    print 'Node %s is already dead. Maybe it\'s just taking a while to show up.' % node
                else:
                    print 'Killing node %s with process id %s' % (node, procId)

                    if node in nodes and nodes[node] != None:
                        execute('kill -s SIGTERM %s' % procId)
                        execute('kill -s SIGTERM %s' % nodes[node].pid)
                    else:
                        print 'Node %s should be dead. DIE!!!' % node
                        execute('kill -9 %s' % procId)
                    nodes[node] = None
            except:
                print 'Syntax: `kill N`, N between 1 & %s' % NUM_NODES

        elif cmds[0] == 's' or cmds[0] == 'start':
            try:
                node = extractNodeFromCmd(cmds)
                assert 0 <= node <= NUM_NODES
                if node not in nodes or nodes[node] == None:
                    start_node(node)
                else:
                    print 'Node %s is already alive... use kill first.' % node
            except:
                print 'Syntax: `start N`, N between 1 & %s' % NUM_NODES

        elif cmds[0] == 'status' or cmds[0] == 'ring':
            (status, output) = execute('sh %s/bin/nodetool %s' % (CASSANDRA_HOME, cmds[0]))
            print output

        elif cmds[0] == 'w' or cmds[0] == 'wait':
            wait()

        elif cmds[0] == 'r' or cmds[0] == 'run':
            try:
                threadCount = int(cmds[1])
                assert threadCount > 0

                print 'Loading data...'
                (status, output) = execute('%s/bin/ycsb load cassandra-7 -p hosts=127.0.0.2 -P %s/workloads/load_workload -p threadcount=%s' % (YCSB_HOME, YCSB_HOME, threadCount))
                print 'Starting test. Check status from testing/ycsb.log'
                logFile = open('%s/ycsb.log' % CASSANDRA_HOME, 'w')
                proc = subprocess.Popen('%s/bin/ycsb run cassandra-7 -p hosts=127.0.0.2 -P %s/workloads/run_workload -p threadcount=%s' % (YCSB_HOME, YCSB_HOME, threadCount), shell=True, stdout=logFile, stderr=logFile)
                print 'Started YCSB RUN as pid=%s' % (proc.pid)
                logFile.close()
                
            except:
                print 'Syntax `run 4` to run YCSB with 4 threads.'

        else:
            print 'Unrecognized command, try again.'

        
