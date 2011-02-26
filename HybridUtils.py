import time
import re
import os
import socket
import sys
import pprint
import inspect

from twisted.internet import protocol,reactor,defer
from twisted.spread import pb
from twisted.internet import abstract,fdesc,threads
from twisted.python import log, failure
from twisted.python.util import untilConcludes
from twisted.enterprise import adbapi 
from MySQLdb import OperationalError, ProgrammingError
from collections import defaultdict

from twisted.web.client import Agent
from twisted.web.http_headers import Headers
import urllib

from twisted.web.iweb import IBodyProducer
from zope.interface import implements
from twisted.internet.defer import succeed

class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass

def httpRequest(url, values={}, headers={}, method='POST'):
    # Construct an Agent.
    agent = Agent(reactor)
    data = urllib.urlencode(values)

    d = agent.request(method,
                      url,
                      Headers(headers),
                      StringProducer(data) if data else None)

    def handle_response(response):
        if response.code == 204:
            d = defer.succeed('')
        else:
            class SimpleReceiver(protocol.Protocol):
                def __init__(s, d):
                    s.buf = ''; s.d = d
                def dataReceived(s, data):
                    s.buf += data
                def connectionLost(s, reason):
                    # TODO: test if reason is twisted.web.client.ResponseDone, if not, do an errback
                    s.d.callback(s.buf)

            d = defer.Deferred()
            response.deliverBody(SimpleReceiver(d))
        return d

    d.addCallback(handle_response)
    return d

def send_email(to, subject, body, from_addr='clusters@hybrid-logic.co.uk'):
    from twisted.mail.smtp import sendmail # XXX This causes blocking on
                                           # looking up our own IP address,
                                           # which will only work if bind is
                                           # running
    from email.mime.text import MIMEText
    host = 'localhost'
    to_addrs = list(set([to, 'tech@hybrid-logic.co.uk']))
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_addr
    msg['To'] = ', '.join(to_addrs)

    d = sendmail(host, from_addr, to_addrs, msg.as_string())
    def success(r):
        print "CRITICAL Success sending email %s" % str(r)
    def error(e):
        print "CRITICAL Error sending email %s" % str(e)
    d.addCallback(success)
    d.addErrback(error)
    return d

def undefaulted(x):
  return dict(
    (k, undefaulted(v))
    for (k, v) in x.iteritems()
  ) if isinstance(x, defaultdict) else x

class SimpleListener():
    def __init__(self, noisy=False):
        self.noisy = noisy
        if self.noisy:
            print "*** Job started at %s\n" % (time.ctime(),)
        self.deferred = defer.Deferred()
       
    def startCmd(self, cmd):
        if self.noisy:
            print "*** Runnning command %s at %s\n" % (cmd, time.ctime())

    def outReceived(self, data):
        if self.noisy:
            print "OUT: " + data.strip()

    def errReceived(self, data):
        if self.noisy:
            print "ERR: " + data.strip()

    def processEnded(self, status):
        if self.noisy:
            print "\n*** Process ended at %s with status code %s\n" % (time.ctime(), status)

    def lastProcessEnded(self):
        if self.noisy:
            print "\n*** Job ended at %s\n" % time.ctime()
        self.deferred.callback(True)

class SpecialLock:
    # TODO: Add support for Deferreds to this so that we can return a deferred
    # which will fire when the queued function eventually runs - and unit test this.

    # Means "is currently running, if I get run again then add to run_next"
    locks = {}  # e.g., {'recompileZones':    True
                #       'recompileVhosts':    True}

    # Means "next time it finishes, start it again with this fn"
    run_again = {}  # e.g., {'recompileZones':    self.recompileZones,
                    #       'recompileVhosts':    self.recompileVhosts}

    def handleLock(self, lock_name, function):
        """Handles a special type of lock:
            * if it's already taken, set run_again = function but in such a way that it only runs again exactly once...
            * when removeLock is run, if run_again exists then do run it again, after setting it to False
        """
        print "*****************************************************"
        print "*** We have been called upon to handle a lock for %s" % lock_name
        
        # If not locked, carry on.
        if not self.locks.has_key(lock_name):
            print "*** Not locked, adding lock and carrying on"
            print "*****************************************************"
            self.locks[lock_name] = True
            return True

        else:
            # Otherwise, schedule a run (possibly overwriting the previous lock, so this only happens once):
            self.run_again[lock_name] = function
            print "*** Locked, setting run_again"
            print "*****************************************************"
            return False

    def removeLock(self, lock_name):

        print "*****************************************************"
        print "*** We have been called upon to remove a lock for %s" % lock_name

        # Unset the lock
        if self.locks.has_key(lock_name):
            del self.locks[lock_name]

        # Run the function again if we've been asked to
        if self.run_again.has_key(lock_name):
            fn = self.run_again[lock_name]
            del self.run_again[lock_name]
            print "*** Running the function again"
            print "*****************************************************"
            fn() # Recurse
        else:
            print "*** Not running the function again"
            print "*****************************************************"


class ExceptionLogger(log.FileLogObserver):
    def emit(self,eventDict):
        is_exception = eventDict.has_key('failure')
        
        connection_error = False
        if is_exception:
            # Some connection errors we don't care about
            connection_error = 'twisted.internet.error.ConnectionRefusedError' in repr(eventDict['failure']) or 'twisted.internet.error.ConnectionLost' in repr(eventDict['failure']) or 'twisted.spread.pb.PBConnectionLost' in repr(eventDict['failure'])

        other_loggable = 'CRITICAL' in repr(eventDict)

        if (is_exception and not connection_error) or other_loggable:
            log.FileLogObserver.emit(self,eventDict)


def all_in(phrases, list):
    """Used to check whether all of the phrases are somewhere in the list
    Useful for error checking when given a return value which may be something
    like:
    
    ['most recent snapshot of rpool/hcfs/mysql-hybridcluster does not\nmatch incremental snapshot',...]

    You can call all_in(['most recent snapshot of','does not','match incremental snapshot'], statuses)
    """

    combined = ''.join(list)

    for phrase in phrases:
        if phrase not in combined:
            return False
    
    return True

def sleep(secs, data=None):
    d = defer.Deferred()
    reactor.callLater(secs, d.callback, data)
    return d

def add_delay(d, secs):
    "Adds a callback to the specified deferred which delays the firing of the callback by the given number of seconds"
    def delay(data):
        print "Starting delay of %f seconds" % secs
        d = sleep(secs, data)
        return d
    d.addCallback(delay)
    return d

class TooManyAttempts(Exception):
    pass

class BackOffUtil(object):
    verbose = False

    def __init__(self, max_retries=10, scale=1.5, tag=None):
        self.attempt = 0
        self.max_retries = max(max_retries, 0)
        self.scale = scale
        self.tag = tag
        if self.verbose:
            print "Starting a %s" % tag

    def backoff(self, result):
        d = defer.Deferred()
        if self.attempt < self.max_retries:
            delay = self.scale ** self.attempt
            self.attempt += 1
            if self.verbose:
                if self.attempt < 2:
                    if self.tag is not None:
                        print ('[%s]' % (self.tag,))
                    print ('backing off by %.2fs..' % (delay,))
                else:
                    print ('%.2fs..' % (delay,))
            reactor.callLater(delay, d.callback, result)
        else:
            if self.verbose:
                if self.tag is not None:
                    print ('[%s]' % (self.tag,))
                print ('(backoff attempt %s excedes limit %s)' % (self.attempt+1, self.max_retries))
            d.errback(failure.Failure(TooManyAttempts('made %d attempts; failing' % (self.attempt,))))
        return d


def is_database(filesystem):
    if filesystem[:6]=='mysql-':
        return filesystem[6:]
    else:
        return False

def is_site(filesystem):
    if filesystem[:5]=='site-':
        return filesystem[5:]
    else:
        return False

def is_mail(filesystem):
    if filesystem[:5]=='mail-':
        return filesystem[5:]
    else:
        return False

def mysql_set(n, s="%s"):
    return "("+(",".join(["%s" for i in range(n)]))+")"

def format_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(float(x)))

def format_time(x):
    return time.strftime('%H:%M:%S',time.gmtime(float(x)))

def clearLogs(type):
    #print "Clearing out %s logs...." % type
    os.system("bash -c \"for X in /opt/HybridCluster/log/%s.log.*; do rm \\$X; done\"" % type) # delete all non-current logfiles

def email_error(subject, body):
    #import platform
    #from twisted.mail.smtp import sendmail
    #from email.mime.text import MIMEText

    #hostname = platform.node()

    #msg = MIMEText(time.ctime()+"\n"+body)
    #msg['Subject'] = 'Error from %s - %s' % (hostname, subject)
    #msg['From'] = "errors@%s" % hostname
    #msg['To'] = "bulk@lukemarsden.net"

    print "CRITICAL: "+body
    #sendmail('smtp.digital-crocus.com', msg['From'], msg['To'], msg.as_string())


class SelectableFile(abstract.FileDescriptor):
    def __init__(self, fname, protocol):
        abstract.FileDescriptor.__init__(self)
        self.fname = fname
        self.protocol = protocol
        self._openFile()

    def _openFile(self):
        self.fp = open(self.fname,'r+') # rob voodoo (opening it read/write stops it blocking - this makes 'sense' because you might want to write to the OS buffer)
        self.fileno = self.fp.fileno
        fdesc.setNonBlocking(self.fp)
        self.protocol.makeConnection(self)
    
    def doRead(self):
        buf = self.fp.read(4096)
        if buf:
            self.protocol.dataReceived(buf)
        else:
            print "File (%s) has closed under our feet, not trying to open it again..." % self.fname
            #reactor.callLater(1,self._openFile)
            #self._openFile()
            #self.protocol.connectionLost()

    def write(self, data):
        pass # what can we do with the data?

    def loseConnection(self):
        self.fp.close()


class BaseUITalker(pb.Root):
    def remote_setRemoteUIConnection(self,ui_connection):
        def callback(conn):
            self.ui_connections.remove(conn)
        ui_connection.notifyOnDisconnect(callback)
        self.ui_connections.append(ui_connection)

class ConnectionDispatcher:
    def __init__(s,context):
        s.context = context
    def callRemote(s, *args, **kw):
        d = DeferredDispatcher(s.context)
        for conn in s.context.ui_connections:
            d.deferreds.append(conn.callRemote(*args, **kw))
        return d

class DeferredDispatcher:
    def __init__(s, context):
        s.context = context
        s.deferreds = []
    def addCallback(s, *args, **kw):
        d = DeferredDispatcher(s.context)
        for deferred in s.deferreds:
            d.deferreds.append(deferred.addCallback(*args, **kw))
        return d
    def addErrback(s, *args, **kw):
        d = DeferredDispatcher(s.context)
        for deferred in s.deferreds:
            d.deferreds.append(deferred.addErrback(*args, **kw))
        return d

def mktup(item):
    # makes a singleton tuple out of item, if it isn't already a tuple
    if type(item)==tuple:
        return item
    else:
        return (item,)

def run_return(cmd):
    import popen2
    #print 'Running command '+cmd
    pipe = popen2.Popen4(cmd)
    ret = (pipe.fromchild.readlines(),pipe.poll())
    # close the pipes
    pipe.fromchild.close()
    pipe.tochild.close()
    return ret

def lookup_ip_mapping_in_config(my_ip):
    import platform
    config = read_config()
    node = platform.node()
    if '.' in node:
        node = node.split('.')[0]
    return config['ips'][node] # i.e. srv-qs3ur on BrightBox

def read_config():
    import yaml
    return yaml.load(open('/etc/cluster/config.yml','r').read())

def get_my_ip(get_local=False):
    my_ip = False
    for line in run_return("/sbin/ifconfig")[0]:
        m = re.match('^.*inet (addr:|)([0-9\.]+) .*(broadcast|Bcast).*$',line) # only check for interfaces which support UDP broadcast...
        if m is not None and m.group(2) != '127.0.0.1' and '172.16.23.' not in m.group(2): # ignore the loopback interface and vmware's NAT interface
            my_ip = m.group(2)
            break

    if my_ip:
        if my_ip.startswith('10.') and not get_local:
            # Assume we're on BrightBox or EC2
            try:
                my_ip = lookup_ip_mapping_in_config(my_ip)
            except (KeyError, IOError):
                print "We're on a 10. network but not actually a cluster node"
                pass
        #print "I reckon my IP is %s" % (my_ip)
        return my_ip
    else:
        #print "HybridUtils.get_my_ip FAILED"
        return '0.0.0.0'

import platform
if platform.win32_ver()[0] == '':
    ip = get_my_ip()
    local_ip = get_my_ip(get_local=True)

def ip_to_alpha(ip):
        alpha = map(lambda x: str(x), range(99))
#        alpha = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z']

        lastdigit = alpha[(int(re.search("[0-9]+\.[0-9]+\.[0-9]+\.([0-9]+)",ip).group(1))%100) % len(alpha)]
        return lastdigit


class curry:
    def __init__(self, fun, *args, **kwargs):
        self.fun = fun
        self.pending = args[:]
        self.kwargs = kwargs.copy()

    def __call__(self, *args, **kwargs):
        if kwargs and self.kwargs:
            kw = self.kwargs.copy()
            kw.update(kwargs)
        else:
            kw = kwargs or self.kwargs

        return self.fun(*(self.pending + args), **kw)

class CallbackProcessProtocol(protocol.ProcessProtocol):
    collector = ''
    def __init__(self, viewer=None):
        self.viewer = viewer

    def outReceived(self,data):
        if self.viewer:
            self.viewer.outReceived(data)
        self.collector += data

    def errReceived(self,data):
        if self.viewer:
            self.viewer.errReceived(data)
        self.collector += data

    def processEnded(self,status):
        if self.viewer:
            self.viewer.processEnded(status)
        self.callback(status,self.collector)

class AlreadyQueuedError(Exception):
    pass

class SlotManager:
    """
    A slot manager handles running a queue of tasks, ensuring that only n of them run simultaneously.
    This ensures that, as well as the server not getting overloaded, we never run out of FDs.

    Each task is a list of commands which get executed sequentially by AsyncExecCmds.
    Each task can be tagged with a tag which is either None - in which case there is no mutual exclusion,
    or a string, in which case no two tasks with the same tag will ever run simultaneously. This
    is useful as a way of locking around a resource, such as a filesystem.

    When a task is queued, its commands are compared to those of the existing tasks in the
    queue. If an identical command is already in the queue, then an AlreadyQueuedException is raised,
    since the same command is due to be run as soon as possible anyway. This helps avoid commands
    unnecessarily building up in the queue.

    Sample code:
    sm = SlotManager(n_slots=4);

    ... lots of other tasks added to the slotmanager...

    sm.queueTask(['cd /var/www','tar cfv backup.tar .','scp backup.tar server...'],
                callback=lambda x: sys.stdout.write('done backup'),
                description="Backup website",
                tag='backup')

    Now this backup will only run when all other tasks tagged with 'backup' are completed *and* there
    is a free slot.

    TODO: Add a timeout argument for each task.

    Internally -
    self.slots contain instances of AsyncExecCmds() or None
    self.queue contain dictionaries of the arguments passed to queueTask
    """
    def __init__(self,n_slots=4,ui=None,target='tasks',timeout=None):
        self.n_slots = n_slots

        self.queue = []
        self.slots = {}

        self.ui = ui
        self.target = target
        self.timeout = timeout

#        self.id_counter = 0

        for x in range(n_slots):
            self.slots[x]=None

    def checkQueue(self):
        #print 'checkQueue called'
        if not None in self.slots.values():
            # No available slots right now, try again later"
            return

        #print 'Got here, queue looks like: '+str(self.queue)

        # XXX: This algorithm is terribly inefficient for long queues.

        negative_new_queue = [] # contains things we've taken out of the queue this time

        for task in self.queue: # run the oldest command first
            running_tags = [t.tag for t in filter(lambda x: x is not None, self.slots.values())] # avoid KeyError
            if task['tag'] is None or task['tag'] not in running_tags:
                # Find the first open slot
                for n,slot in self.slots.items():
                    if slot is None:
                        #print "We're good to go, this tag isn't currently running and there's a free slot"
                        # Spawning task "+task['description']+" in slot "+str(n)
                        self.startTask(task,n)
                        negative_new_queue.append(task)
                        break # don't continue the for loop
            else:
                print "This tag (" + task['tag'] + ") is currently running, so not started"

        self.queue = [x for x in self.queue if x not in negative_new_queue]
        
        """old_queue = self.queue

        self.queue = []
        for x in old_queue:
            if x not in negative_new_queue:
                self.queue.append(x)"""
    
        def callback():
            pass

    def startTask(self,task,n):
        """Starts task described by dictionary task in slot n, which is assumed to be None at time of calling"""
        task_callback = task['callback']
        print "Running task %s" % (task['description'])

        def custom_callback(statuses,outputs=[],parameters={}):
            # Set the slot to empty, then run the task's callback
            # Got callback on "+task['description']+" of "+str(statuses)
            #print "Updating cell for task "+str(task['id'])+" with "+str(statuses)
            if self.ui is not None:
                try:
                    self.ui.callRemote('updateCell',item=(task['id'],ip,task['description']),column=3,type=self.target,value="Finished: "+str(statuses),outputs=outputs)
                except Exception,e:
                    print "Carrying on after exception in updateCell: "+str(e)
            
            if task_callback is not None:
                args_for_callback = {}

                # Introspection: Nasty or nice? I can't decide
                if 'parameters' in inspect.getargspec(task_callback).args:
                    args_for_callback['parameters'] = parameters
                if 'outputs' in inspect.getargspec(task_callback).args:
                    args_for_callback['outputs'] = outputs
                if 'cmds' in inspect.getargspec(task_callback).args:
                    args_for_callback['cmds'] = task['cmds']

                task_callback(statuses, **args_for_callback)

            self.slots[n]=None
            # check if there's anything we want to run now that we've freed up a slot
            try:
                self.checkQueue()
            except Exception,e:
                print "CRITICAL: Exception caught when running checkQueue "+str(e)

        # Starting task "+task['description']

        p = {"now":str(time.time())}

        if self.ui is not None:
            try:
                self.ui.callRemote('updateCell',item=(task['id'],ip,task['description']),column=3,type='tasks',value="Running in slot "+str(n))
            except Exception,e:
                print "Carrying on after exception in updateCell: "+str(e)

        # apply the parameter transformation to elements which aren't functions (if they're functions, they get called in a thread)
        for i,cmd in enumerate(task['cmds']):
            if type(cmd) == str or type(cmd) == unicode:
                task['cmds'][i] = task['cmds'][i] % p

        print "!" * 80
        pprint.pprint(task)
        print "!" * 80

        self.slots[n]=AsyncExecCmds(
                task['cmds'],
                callback=curry(custom_callback, parameters=p),
                verbose=task['cmds'], # XXX !?
                ui=self.ui,
                description=task['description'],
                tag=task['tag'],
                id=task['id'])

    # XXX UI argument (below) is deprecated and not used, retained for compatibility
    def queueTask(self,cmds,callback=None,verbose=0,ui=None,description='<no description>',tag=None,id=None,skip_uniqueness_check=False, errback=None): # TODO: Make this return a deferred, obviously

        if not skip_uniqueness_check:
            for task in self.queue:
                if task['cmds']==cmds:
                    print "Error - this task already queued: "+str(cmds)

                    if errback is not None:
                        errback()
                    return False

            # The following seems to cause trouble
            """for slot in self.slots.values():
                if slot is not None:
                    if slot.cmds == cmds:
                        print "CRITICAL Error - this task CURRENTLY RUNNING: "+str(cmds)
                    
                        if errback is not None:
                            errback()
                        return False"""


#                    raise AlreadyQueuedError

        # append this task to the queue

        id = int(long(time.time()*1000) % 999999999)
        self.queue.append({'cmds':cmds,'callback':callback,'verbose':verbose,'description':description,'tag':tag,'id':id})
#        self.id_counter += 1

        # since commands are always unique, we can identify the command in the UI by the descr & cmdlist
        # XXX if skip_uniqueness_check == True the above assumption is false

        # Add the task to the UI
        if self.ui is not None:
            def callback(*args):
                self.ui.callRemote('updateCell',item=(id,ip,description),column=3,type='tasks',value="Queued")

            try:
                self.ui.callRemote('addTask',{(id,ip,description): map(lambda cmd: (id,'',cmd),cmds)}).addCallback(callback)

            except Exception, e:
                print "Caught "+str(e)+" when updating UI but carrying on regardless"

        self.checkQueue()

class AsyncExecCmds: # for when you want to run things one after another

    def __init__(self,cmds,callback=None,verbose=0,ui=None,description='<no description>',tag=None,id=None,auto_run=True,cmd_prefix=None,viewer=None):
        "Asynchronously run a list of commands, return a list of status codes"
        self.cmds = cmds
        self.callback = callback
        self.statuses = []
        self.outputs = []
        self.verbose = verbose
        self.ui = ui
        self.description = description
        self.tag = tag
        self.id = id
        self.cmd_prefix = cmd_prefix
        self.viewer = viewer

        if auto_run:
            self.runNextCmd()

    def getDeferred(self):
        "Return a deferred which fires when the callback would have been called."
        d = defer.Deferred()
        def callback(statuses,outputs):
            try:
                d.callback((statuses,outputs))
            except failure.NoCurrentExceptionError, e:
                print "*"*80
                print "How odd, failure.NoCurrentExceptionError when I was trying to callback with:"
                print statuses, outputs
                print "*"*80
                print e
                print "*"*80
                
        self.callback = callback
        return d

    def deferredOutputLines(self):
        """Utility function so you can just do:

        outputs = yield AsyncExecCmds(['some cmd']).deferredOutputLines()
        for line in outputs[0]: # output of the first command
            # do something with this line
        """
        d = defer.Deferred()
        def callback(statuses,outputs):
            d.callback([x.strip().split('\n') for x in outputs])
        self.callback = callback
        return d


    def runNextCmd(self):
        "Pops the first command in the current list, then runs it, recursing in the callback"
        #print self.cmds
        if self.cmds == []: # we're done, return the list of statuses
            if self.viewer:
                self.viewer.lastProcessEnded() # Allow the viewer to do cleanup (like closing file handles)

            def to_string(status):
                if type(status) in [int,str,unicode,bool]:
                    return status
                if "twisted.internet.error.ProcessDone" in repr(status):
                    return "Done"
                if "twisted.internet.error.ProcessTerminated" in repr(status):
                    return "Failed"
                return repr(status)
            if self.callback is not None:
                self.callback(map(to_string,self.statuses),self.outputs)
            return

        cmd = self.cmds.pop(0)

        if self.viewer:
            self.viewer.startCmd(cmd)

        cpp = CallbackProcessProtocol(self.viewer)

        def cmd_callback(status,output):
            self.statuses.append(status)
            self.outputs.append(output)
            self.runNextCmd()

        cpp.callback = cmd_callback

        if type(cmd) in [str,unicode]: # it's a system call
            # run everything through bash so that fun things like pipes work
            if cmd[0:16] in ['/usr/bin/ssh']:
                prefix = ''
            else:
                prefix = '/usr/local/bin/sudo '

            # override this logic for times when the app wants to choose whether to sudo or not
            if self.cmd_prefix is not None:
                prefix = self.cmd_prefix

            if self.verbose>=0:
                print "running "+prefix+cmd
            reactor.callLater(0,reactor.spawnProcess,cpp,'/bin/bash',['/bin/sh', '-c',(prefix+cmd).encode('ascii','ignore')])

        else: # assume it's a function, find out whether it's advertised itself as returning a deferred (assume it isn't)

            def generic_callback(x='Finished'):
                cmd_callback(x)

            returns_deferred = False

            if hasattr(cmd,'returns_deferred'):
                returns_deferred = cmd.returns_deferred

            if returns_deferred:
                # execute it directly (i.e. database queries etc)
                d = cmd()
            else:
                # pop it in a thread
                d = threads.deferToThread(cmd)

            d.addCallback(generic_callback)

    def run(self):
        self.runNextCmd()


class AsyncExecCmd(AsyncExecCmds):
    def __init__(self,cmd,callback=None,verbose=0,ui=None,description='<no description>'):
        AsyncExecCmds.__init__(self,[cmd],callback,verbose,ui,description)

"""
        self.verbose = verbose
        if self.verbose:
            print cmd,' '.join(args)
        cpp = CallbackProcessProtocol()
        cpp.callback=callback
        reactor.spawnProcess(cpp,cmd,args)"""

def ntp():
    import struct, telnetlib, time, socket

    EPOCH = 2208988800L

    try:
        fromNTP = telnetlib.Telnet('time.nrc.ca', 37).read_all()
        #the string's a big endian uInt
        remoteTime = struct.unpack('!I', fromNTP )[0] - EPOCH

        # NTP time converted to time struct & made friendly
        return time.gmtime(remoteTime)
    
    except socket.gaierror:
        print "WARNING: NTP server unavailable"
        return False

def nice_time(t):
    """ turns a unix timestamp into human-readable AND unix timestamp """
    return time.ctime(t)+" "+str(t)

def shellquote(s):
        return "'" + s.replace("'", "'\\''") + "'"

@defer.inlineCallbacks
def safe_write(file, contents, perms=None): # FIXME: this is not really a "safe" write any more (the move and permissions fix cannot be done atomically), but as long as we wait until it's finished before reloading the program, we'll be okay
    tmpname = '/tmp/'+file.replace('/','%')+".tmp"
    fp = open(tmpname, "w")
    fp.write(contents)
    fp.close()
    yield AsyncExecCmds(["/bin/mv %s %s" % (shellquote(tmpname), shellquote(file))]).getDeferred()
    if perms is not None:
        yield AsyncExecCmds(["/bin/chmod %s %s" % (perms, shellquote(file))]).getDeferred()

def hex2ip(sender):
    _,hex,priv = sender.split('#')
    quads = [hex[0:2],hex[2:4],hex[4:6],hex[6:8]]
    ip = '.'.join(map(lambda x: str(int(x,16)),quads))
    return ip

#def test_callback(data):
#    print data
#    print map(lambda x: x, data)

#AsyncExecCmds(['/bin/false','/bin/true','/bin/false'],test_callback)
#AsyncExecCmd('/bin/false',['/bin/false'],test_callback)
#reactor.run()
#raise SystemExit

def natsort(list_):
    try:
        # decorate
        tmp = [ (int(re.search('\d+', i).group(0)), i) for i in list_ ]
        tmp.sort()
        # undecorate
        return [ i[1] for i in tmp ]
    except AttributeError:
        list_.sort()
        return list_

def setproctitle(name):
    try:
        from ctypes import cdll, byref, create_string_buffer
        libc = cdll.LoadLibrary('libc.so.7')
        libc.setproctitle(name+'\0')
    except:
        print "Unable to set process name, not on FreeBSD?"

def parse_zfs_list(data,zpool):
    zfs_data = {}
    for line in data.split('\n'):
        match = re.match(zpool+'/hcfs/([^/@]+)@?([0-9\.]*)',line.strip())
        if match is not None:
            site,snapshot = match.groups()
            if site not in zfs_data.keys():
                zfs_data[site] = []
            if snapshot != '':
                zfs_data[site].append(snapshot)
    return zfs_data

def remote_prefix(username,ip_to,sudo='sudo'):
    return '/usr/bin/env ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no '+username+'@'+ip_to+' -i /home/hybrid/.ssh/id_rsa '+sudo+' ' # NB should we remove the id_rsa bit? I think it's a solarisism


class Spinner:
    def __init__(self, type=0):
        if type == 0:
            self.char = ['.', 'o', 'O', 'o']
        else:
            self.char = ['|', '/', '-', '\\', '-']
        self.len  = len(self.char)
        self.curr = 0

    def Get(self):
        self.curr = (self.curr + 1) % self.len
        str = self.char[self.curr]
        return str

    def Print(self):
        self.curr = (self.curr + 1) % self.len
        str = self.char[self.curr]
        sys.stdout.write("\b \b%s" % str)
        sys.stdout.flush()

    def Done(self):
        sys.stdout.write("\b \b")
        sys.stdout.flush()
