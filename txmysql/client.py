from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from twisted.internet import reactor, defer
from protocol import MySQLProtocol # One instance of this per actual connection to MySQL
from txmysql import util, error
from twisted.python.failure import Failure
from twisted.internet.error import TimeoutError
from twisted.python import log
import time

DEBUG = False

def _escape(query, args=None):
    if not args:
        return query
    escaped_args = []
    for arg in args:
        escaped_args.append("'%s'" % str(arg).replace("'", "\\'"))
    parts = ("[%s]" % str(query)).split('%s') # Add square brackets to
                                              # guarantee that %s on the end or
                                              # beginning get a corresponding
                                              # split
    if len(escaped_args) + 1 != len(parts):
        raise TypeError, 'not enough arguments for MySQL format string'
    # Pad args so that there are an equal number of args and query
    escaped_args.insert(0, '')
    if len(parts) != len(escaped_args):
        raise TypeError, 'INTERNAL ERROR'
    # Now interpolate and remove the square brackets
    return (''.join(x + y for x, y in zip(escaped_args, parts)))[1:-1]

class MySQLConnection(ReconnectingClientFactory):
    """
    Takes the responsibility for the reactor.connectTCP call away from the user.

    Lazily connects to MySQL only when a query is run and stays connected only
    for up to idle_timeout seconds.

    Handles reconnecting on disconnection if there are queries which have not
    yet had results delivered.

    When excuting a query, waits until query_timeout expires before giving up
    and reconnecting (assuming this MySQL connection has "gone dead"). If
    retry_on_error == True, attempts the query again once reconnected.  If not,
    returns a Failure to the user's deferred.

    Also accepts a list of error strings from MySQL which should be considered
    temporary local failures, which should trigger a reconnect-and-retry rather
    than throwing the failure up to the user. These may be application-specific.

    Note that this and MySQLProtocol both serialise database access, so if you
    try to execute multiple queries in parallel, you will have to wait for one
    to finish before the next one starts. A ConnectionPool inspired by
    http://hg.rpath.com/rmake/file/0f76170d71b7/rmake/lib/dbpool.py is coming
    soon to solve this problem (thanks gxti).
    """

    protocol = MySQLProtocol
    
    def disconnect(self):
        """
        Close the connection and kill all the reconnection attempts
        """
        self.stopTrying()
        self.stateTransition(state='disconnecting')
        if self.client:
            # Do some clean-up
            self.client.setTimeout(None)
            self.client.transport.loseConnection()

    def __init__(self, hostname, username, password, database=None,
            connect_timeout=None, query_timeout=None, idle_timeout=None,
            retry_on_error=False, temporary_error_strings=[], port=3306):

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.query_timeout = query_timeout
        self.idle_timeout = idle_timeout
        self.retry_on_error = retry_on_error
        self.temporary_error_strings = temporary_error_strings
        self.deferred = defer.Deferred() # This gets fired when we have a new
                                         # client which just got connected
        self._current_selected_db = None

        self.state = 'disconnected'
        self.client = None # Will become an instance of MySQLProtocol
                           # precisely when we have a live connection
 
        # Attributes relating to the queue
        self._pending_operations = []
        self._current_operation = None
        self._current_operation_dfr = None
        self._current_user_dfr = None

        # Set when we get disconnected, so that we know to attempt
        # a retry of a failed connection
        self._error_condition = False

    def runQuery(self, query, query_args=None):
        user_dfr = defer.Deferred()
        self._pending_operations.append((user_dfr, self._doQuery, query, query_args))
        self._checkOperations()
        if DEBUG:
            print "    Appending query \"%s\" which is due to fire back on new user deferred %s" % (query, user_dfr)
        return user_dfr

    def selectDb(self, db):
        user_dfr = defer.Deferred()
        self._pending_operations.append((user_dfr, self._doSelectDb, db, None))
        self._checkOperations()
        self.database = db
        if DEBUG:
            print "    Appending selectDb \"%s\" which is due to fire back None on new user deferred %s" % (db, user_dfr)
        return user_dfr

    def runOperation(self, query, query_args=None):
        user_dfr = defer.Deferred()
        self._pending_operations.append((user_dfr, self._doOperation, query, query_args))
        self._checkOperations()
        if DEBUG:
            print "    Appending operation \"%s\" which is due to fire back None on new user deferred %s when complete" % (query, user_dfr)
        return user_dfr

    def _retryOperation(self):
        if DEBUG:
            print "    Running retryOperation on current operation %s" % str(self._current_operation)

        if not self._current_operation:
            # Oh, we weren't doing anything
            return

        user_dfr, func, query, query_args = self._current_operation
        operation_dfr = func(query, query_args)
        self._current_operation_dfr = operation_dfr
        self._current_user_dfr = user_dfr

        operation_dfr.addBoth(self._doneQuery)

        # Jump back into the game when that operation completes (done_query_error returns none
        # so the callback, not errback gets called)
        operation_dfr.addCallback(self._checkOperations)

    def _doneQuery(self, data):
        # The query deferred has fired
        if self._current_user_dfr:
            if isinstance(data, Failure):
                if data.check(error.MySQLError):
                    if data.value.args[0] in self.temporary_error_strings:
                        if DEBUG:
                            print "    Found %s in %s, reconnecting and retrying" % (data.value.args[0], self.temporary_error_strings)
                        self.client.transport.loseConnection()
                        return
                if DEBUG:
                    print "    Query failed with error %s, errback firing back on %s" % (data, self._current_user_dfr)
                self._current_user_dfr.errback(data)
            else:
                if DEBUG:
                    print "    Query is done with result %s, firing back on %s" % (data, self._current_user_dfr)
                self._current_user_dfr.callback(data)
            self._current_user_dfr = None
        else:
            print "    WARNING! Current user deferred was None when a query fired back with %s - there should always be a user deferred to fire the response to..." % data
        self._error_condition = False
        self._current_operation = None
        self._current_operation_dfr = None
        # If that was a failure, the buck stops here, returning None instead of the failure stops it propogating

    def _checkOperations(self, data=None):
        """
        Takes one thing off the queue and runs it, if we can.  (i.e. if there
        is anything to run, and we're not waiting on a query to fire back to
        the user right now, i.e. current user deferred exists)
        """
        if DEBUG:
            print "    Running checkOperations on the current queue %s while current operation is %s" % (str(self._pending_operations), str(self._current_operation))
        #print "    Got to _checkOperations"
        if self._pending_operations and not self._current_user_dfr:
            # Take the next pending operation off the queue
            user_dfr, func, query, query_args = self._pending_operations.pop(0)
            # Store its parameters in case we need to run it again
            self._current_operation = user_dfr, func, query, query_args
            if DEBUG:
                print "    Setting current operation to %s" % str(self._current_operation)
                print "    About to run %s(%s, %s) and fire back on %s" % (str(func), str(query), str(query_args), str(user_dfr))
            # Actually execute it, operation_dfr will fire when the database returns
            operation_dfr = func(query, query_args)
            #operation_dfr.addErrback(log.err)
            # Store a reference to the current operation (there's gonna be only
            # one running at a time)
            self._current_operation_dfr = operation_dfr
            self._current_user_dfr = user_dfr

            operation_dfr.addBoth(self._doneQuery)

            # Jump back into the game when that operation completes (done_query_error returns none
            # so the callback, not errback gets called)
            operation_dfr.addBoth(self._checkOperations)

        return data

    def stateTransition(self, data=None, state='disconnected', reason=None):
        new_state = state
        old_state = self.state

        if new_state == old_state:
            # Not a transition, heh
            return

        if DEBUG:
            print "    Transition from %s to %s" % (self.state, new_state)
        
        self.state = new_state
        
        # connected => not connected
        if old_state == 'connected' and new_state != 'connected':
            if DEBUG:
                print "    We are disconnecting..."
            # We have just lost a connection, if we're in the middle of
            # something, send an errback, unless we're going to retry 
            # on reconnect, in which case do nothing
            if not self.retry_on_error and self._current_operation:
                if DEBUG:
                    print "    Not retrying on error, current user deferred %s about to get failure %s" % (self._current_user_dfr, reason)
                if self._current_user_dfr and not self._current_user_dfr.called:
                    if DEBUG:
                        print "    Current user deferred exists and has not been called yet, running errback on deferred %s about to get failure %s" % (self._current_user_dfr, reason)
                    self._current_user_dfr.errback(reason)
                    self._current_user_dfr = None
                    self._current_operation = None
                    self._current_operation_dfr = None
                else:
                    if DEBUG:
                        print "    Current user deferred has already been fired in error handler, not doing anything"

        # not connected => connected
        if old_state != 'connected' and new_state == 'connected':
            #print "    In branch 2"
            if DEBUG:
                print "    We are connected..."
            # We have just made a new connection, if we were in the middle of
            # something when we got disconnected and we want to retry it, retry
            # it now
            if self._current_operation and self._error_condition:
                #print "    In branch 2.1, current operation is %s" % str(self._current_operation)
                if self.retry_on_error:
                    if DEBUG:
                        print "    Retrying on error %s, with current operation %s" % (str(reason), str(self._current_operation))
                    # Retry the current operation
                    #print "    In branch 2.1.1"
                    self._retryOperation()
                else:
                    if DEBUG:
                        print "    Not retrying on error, connection made, nothing to do."
           
            else:
                # We may have something in our queue which was waiting until we became connected
                #print "    In branch 2.2"
                if DEBUG:
                    print "    Connected, check whether we have any operations to perform"
                self._checkOperations()
        
        return data

    def clientConnectionFailed(self, connector, reason):
        if DEBUG:
            print "    Got clientConnectionFailed for reason %s" % str(reason)
        self._error_condition = True
        if self.state != 'disconnecting':
            self.stateTransition(state='connecting', reason=reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    
    def clientConnectionLost(self, connector, reason):
        if DEBUG:
            print "    Got clientConnectionLost for reason %s" % str(reason)
        self._error_condition = True
        if self.state != 'disconnecting':
            self.stateTransition(state='connecting', reason=reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    
    @defer.inlineCallbacks
    def _begin(self):
        if self.state == 'disconnected':
            if DEBUG:
                print "    Connecting after being disconnected, with connection timeout %s" % self.connect_timeout
            self.stateTransition(state='connecting')
            # TODO: Use UNIX socket if string is "localhost"
            reactor.connectTCP(self.hostname, self.port, self, timeout=self.connect_timeout)
            yield self.deferred # will set self.client
            yield self.client.ready_deferred
        elif self.state == 'connecting':
            if DEBUG:
                print "    Yielding on a successful connection, deferred is %s" % self.deferred
            yield self.deferred
            if DEBUG:
                print "    Yielding on a successful ready deferred"
            yield self.client.ready_deferred
        elif self.state == 'connected':
            if DEBUG:
                print "    Already connected when a query was attempted, well that was easy"
            pass
    
    def buildProtocol(self, addr):
        if DEBUG:
            print "    Building a new MySQLProtocol instance for connection to %s, attempting to connect, using idle timeout %s" % (addr, self.idle_timeout)
        #print "    Running buildprotocol for %s" % addr
        p = self.protocol(self.username, self.password, self.database,
                idle_timeout=self.idle_timeout)
        p.factory = self
        self.client = p
        #print self.client.ready_deferred
        self.deferred.callback(self.client)
        self.deferred = defer.Deferred()
        def when_connected(data):
            if DEBUG:
                print "    Connection just successfully made, and MySQL handshake/auth completed. About to transition to connected..."
            self.stateTransition(state='connected')
            return data
        self.client.ready_deferred.addCallback(when_connected)
        self.resetDelay()
        return p

    @defer.inlineCallbacks
    def _doQuery(self, query, query_args=None): # TODO query_args
        if DEBUG:
            print "    Attempting an actual query \"%s\"" % _escape(query, query_args) 
        yield self._begin()
        result = yield self.client.fetchall(_escape(query, query_args))
        defer.returnValue(result)

    @defer.inlineCallbacks
    def _doOperation(self, query, query_args=None): # TODO query_args
        if DEBUG:
            print "    Attempting an actual operation \"%s\"" % _escape(query, query_args)
        yield self._begin()
        yield self.client.query(_escape(query, query_args))

    @defer.inlineCallbacks
    def _doSelectDb(self, db, ignored):
        if DEBUG:
            print "    Attempting an actual selectDb \"%s\"" % db
        yield self._begin()
        yield self.client.select_db(db)
