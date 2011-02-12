from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from twisted.internet import reactor, defer
from protocol import MySQLProtocol # One instance of this per actual connection to MySQL
from txmysql import util, error
from twisted.python.failure import Failure
from twisted.python import log
import time

class MySQLConnection(ReconnectingClientFactory):
    """
    Takes the responsibility for the reactor.connectTCP call away from the user.

    Lazily connects to MySQL when a query is run and stays connected only
    for up to idle_timeout seconds.

    Handles reconnecting if there are queries which have not yet had results
    delivered.

    When excuting a query, waits until query_timeout expires before giving up
    and reconnecting (assuming this MySQL connection has "gone dead"). If 
    retry_on_error == True, attempts the query again once reconnected.

    Also accepts a list of error strings from MySQL which should be considered
    temporary local failures which should trigger a reconnect-and-retry rather
    than throwing the failure up to the user. These may be application-specific.

    Note that this and MySQLProtocol both serialise database access, so if you
    try to execute multiple queries in parallel, you will have to wait for one
    to finish before the next one starts. A ConnectionPool inspired by
    http://hg.rpath.com/rmake/file/0f76170d71b7/rmake/lib/dbpool.py is coming
    soon to solve this problem (thanks gxti).
    """

    protocol = MySQLProtocol

    def __init__(self, hostname, username, password, database=None,
            connect_timeout=None, query_timeout=None, idle_timeout=None,
            retry_on_error=False, temporary_error_strings=[], port=3306):

        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.query_timeout = query_timeout
        self.idle_timeout = idle_timeout
        self.retry_on_error = retry_on_error
        self.temporary_error_strings = temporary_error_strings
        self.port = port
        self.deferred = defer.Deferred() # This gets called when we have a new
                                         # client which just got connected

        self.state = 'disconnected'
        self.client = None # Will become an instance of MySQLProtocol
                           # precisely when we have a live connection
        self._last_user_dfr = None
  
        # Attributes relating to the queue
        self._pending_operations = []
        self._current_operation = None
        self._current_operation_dfr = None
        self._current_user_dfr = None

        self._error_condition = False

    def runQuery(self, query, query_args=None):
        user_dfr = defer.Deferred()
        user_dfr.addErrback(log.err)
        self._pending_operations.append((user_dfr, self._doQuery, query, query_args))
        self._checkOperations()
        return user_dfr

    def runOperation(self, query, query_args=None):
        user_dfr = defer.Deferred()
        self._pending_operations.append((user_dfr, self._doOperation, query, query_args))
        self._checkOperations()
        return user_dfr

    def _retryOperation(self):
        #print "Got to retryOperation"
        #import traceback
        #print traceback.print_stack()

        # We got disconnected during an operation. Okay, first make sure the user deferred never gets fired
        # inadvertently if the current operation fires yet...

        #if self._current_operation_dfr:
        #    self._current_operation_dfr.cancel()
        #self._current_operation_dfr = None

        if not self._current_operation:
            #print "Oh, we weren't doing anything"
            # Oh, we weren't doing anything
            return

        user_dfr, func, query, query_args = self._current_operation
        operation_dfr = func(query, query_args)
        self._current_operation_dfr = operation_dfr
        self._current_user_dfr = user_dfr

        def done_query(data):
            # The query deferred has fired
            self._current_operation = None
            self._current_operation_dfr = None
            self._current_user_dfr.callback(data)
            self._current_user_dfr = None
            self._error_condition = False

        def done_query_error(failure):
            self._current_operation = None
            self._current_operation_dfr = None
            self._current_user_dfr.errback(failure)
            self._current_user_dfr = None
            self._error_condition = False
            # The buck stops here, returning None instead of the failure stops it propogating

        operation_dfr.addCallback(done_query)
        operation_dfr.addErrback(done_query_error)

        # Jump back into the game when that operation completes (done_query_error returns none
        # so the callback, not errback gets called)
        operation_dfr.addCallback(self._checkOperations)


    def _checkOperations(self, data=None):
        """
        Takes one thing off the queue and runs it, if we can.
        (i.e. if there is anything to run, and we're not running anything right now)
        """
        #print "Got to _checkOperations"
        if self._pending_operations and not self._current_operation:
            # Take the next pending operation off the queue
            user_dfr, func, query, query_args = self._pending_operations.pop(0)
            # Store its parameters in case we need to run it again
            self._current_operation = user_dfr, func, query, query_args
            # Actually execute it, operation_dfr will fire when the database returns
            #print "About to run %s(%s, %s) and fire back on %s" % (str(func), str(query), str(query_args), str(user_dfr))
            operation_dfr = func(query, query_args)
            operation_dfr.addErrback(log.err)
            # Store a reference to the current operation (there's gonna be only
            # one running at a time)
            self._current_operation_dfr = operation_dfr
            self._current_user_dfr = user_dfr

            def done_query(data):
                # The query deferred has fired
                self._current_operation = None
                self._current_operation_dfr = None
                if self._current_user_dfr:
                    self._current_user_dfr.callback(data)
                self._current_user_dfr = None

            def done_query_error(failure):
                #print "_checkOperations done_query_error got %s" % str(failure)
                self._current_operation = None
                self._current_operation_dfr = None
                if self._current_user_dfr:
                    self._current_user_dfr.errback(failure)
                self._current_user_dfr = None
                # The buck stops here, returning None instead of the failure stops it propogating

            operation_dfr.addCallback(done_query)
            operation_dfr.addErrback(done_query_error)

            # Jump back into the game when that operation completes (done_query_error returns none
            # so the callback, not errback gets called)
            operation_dfr.addBoth(self._checkOperations)

        return data

    def stateTransition(self, data=None, state='disconnected', reason=None):
        new_state = state
        if new_state == self.state:
            # Not a transition, heh
            return

        #print "Transition from %s to %s" % (self.state, new_state)
        
        # connected => not connected
        if self.state == 'connected' and new_state != 'connected':
            #print "In branch 1"
            # We have just lost a connection, if we're in the middle of
            # something, send an errback, unless we're going to retry 
            # on reconnect, in which case do nothing
            if not self.retry_on_error and self._current_operation:
                self._current_user_dfr.errback(reason)
                self._current_user_dfr = None
                self._current_operation = None
                self._current_operation_dfr = None

        # not connected => connected
        if self.state != 'connected' and new_state == 'connected':
            #print "In branch 2"
            # We have just made a new connection, if we were in the middle of
            # something when we got disconnected and we want to retry it, retry
            # it now
            if self._current_operation and self._error_condition:
                #print "In branch 2.1, current operation is %s" % str(self._current_operation)
                if self.retry_on_error:
                    # Retry the current operation
                    #print "In branch 2.1.1"
                    self._retryOperation()
                else:
                    #print "In branch 2.1.2"
                    # Abort the current execution, sending the failure
                    # to the user deferred
                    self._current_user_dfr.errback(reason)
                    self._current_user_dfr = None
                    self._current_operation = None
                    self._current_operation_dfr = None
           
            else:
                # We may have something in our queue which was waiting until we became connected
                #print "In branch 2.2"
                self._checkOperations()
        
        self.state = new_state
        return data

    def buildProtocol(self, addr):
        #print "Running buildprotocol for %s" % addr
        p = self.protocol(self.username, self.password, self.database,
                idle_timeout=self.idle_timeout)
        p.factory = self
        self.client = p
        #print self.client.ready_deferred
        self.deferred.callback(self.client)
        self.deferred = defer.Deferred()
        def when_connected(data):
            #print "About to do stateTransition, whee.."
            self.stateTransition(state='connected')
            return data
        self.client.ready_deferred.addCallback(when_connected)
        self.resetDelay()
        return p

    def clientConnectionFailed(self, connector, reason):
        #print "GOT CLIENTCONNECTIONFAILED"
        self._error_condition = True
        self.stateTransition(state='connecting', reason=reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    
    def clientConnectionLost(self, connector, reason):
        #print "GOT CLIENTCONNECTIONLOST %s" % reason
        self._error_condition = True
        self.stateTransition(state='connecting', reason=reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    
    @defer.inlineCallbacks
    def _begin(self):
        if self.state == 'disconnected':
            self.stateTransition(state='connecting')
            # TODO: Use UNIX socket if string is "localhost"
            reactor.connectTCP(self.hostname, self.port, self)
            yield self.deferred # will set self.client
            yield self.client.ready_deferred
        elif self.state == 'connecting':
            yield self.deferred
            yield self.client.ready_deferred
        elif self.state == 'connected':
            pass

    @defer.inlineCallbacks
    def _doQuery(self, query, query_args=None): # TODO query_args
        yield self._begin()
        result = yield self.client.fetchall(query)
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _doOperation(self, query, query_args=None): # TODO query_args
        yield self._begin()
        yield self.mysql_connection.client.query(query)

