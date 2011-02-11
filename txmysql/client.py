from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from twisted.internet import reactor, defer
from protocol import MySQLProtocol # One instance of this per actual connection to MySQL
from txmysql import util, error
from twisted.python.failure import Failure

def operation(func):
    """
    Serialise the running of these functions
    """
    func = defer.inlineCallbacks(func)
    def wrap(self, *a, **kw):
        return self._do_operation(func, self, *a, **kw)
    return wrap

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
        self._operations = []
        self._current_operation = None # In case we want to retry the query
        self._current_operation_dfr = None
        self.deferred = defer.Deferred() # This gets called when we have a new
                                         # client which just got connected

        self.state = 'disconnected'
        self.client = None # Will become an instance of MySQLProtocol
                           # precisely when we have a live connection
        self._last_user_dfr = None

    def stateTransition(self, _ign=None, state='disconnected', reason=None):
        new_state = state
        if new_state == self.state:
            # Not a transition, heh
            return

        print "Transition from %s to %s" % (self.state, new_state)
        
        # connected => not connected
        if self.state == 'connected' and new_state != 'connected':
            print "In branch 1"
            # We have just lost a connection, if we're in the middle of
            # something, send an errback, unless we're going to retry 
            # on reconnect, in which case do nothing
            if not self.retry_on_error and self._current_operation:
                self._current_operation_dfr.errback(reason)

        # not connected => connected
        if self.state != 'connected' and new_state == 'connected':
            print "In branch 2"
            # We have just made a new connection, if we were in the middle of
            # something when we got disconnected and we want to retry it, retry
            # it now
            if self.retry_on_error and self._current_operation:
                _, f, a, kw = self._current_operation
                # user_dfr is the actual deferred which we need to send the
                # result back to the user on
                d = f(*a, **kw)
                print "About to run %s with %s %s" % (f, a, kw)
                def done_query(data):
                    print "Just come back from recovered query with data %s" % data
                    print "And here are the operations..."
                    print self._operations
                    print self._current_operation
                    print self._current_operation_dfr
                    print self._last_user_dfr
                    print "?"*80
                    user_dfr = self._current_operation[0]
                    user_dfr.callback(data)
                    self._current_operation = None
                    self._current_operation_dfr = None
                    self._update_operations() # Continue with any further pending queries
                d.addCallback(done_query)

            else:
                # Abort the current execution and run the next one, if any
                self._current_operation = None
                self._current_operation_dfr = None
                self._update_operations()
        
        self.state = new_state

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

    def _end(self):
        pass

    def buildProtocol(self, addr):
        print "Running buildprotocol for %s" % addr
        p = self.protocol(self.username, self.password, self.database,
                idle_timeout=self.idle_timeout)
        p.factory = self
        print "*!"*150
        self.client = p
        print self.client.ready_deferred
        print "*!"*150
        self.deferred.callback(self.client)
        self.deferred = defer.Deferred()
        self.client.ready_deferred.addCallback(self.stateTransition, state='connected')
        self.resetDelay()
        return p

    def clientConnectionFailed(self, connector, reason):
        print "GOT CLIENTCONNECTIONFAILED"
        self.stateTransition(state='connecting', reason=reason)
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    
    def clientConnectionLost(self, connector, reason):
        print "GOT CLIENTCONNECTIONLOST %s" % reason
        self.stateTransition(state='connecting', reason=reason)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    #def retry(...): CHECK IF WE HAVE PENDING QUERIES AND ONLY RECONNECT IF WE DO

    @operation
    def runQuery(self, query): # TODO query_args
        yield self._begin()
        result = yield self.client.fetchall(query)
        print "@"*80
        print result
        print "@"*80
        yield self._end()
        defer.returnValue(result)

    @operation
    def runOperation(self, query): # TODO query_args
        yield self._begin()
        yield self.mysql_connection.client.query(query)
        yield self._end()

    # TODO: Put this code in a common base class
    def _update_operations(self, _result=None):
        """
        Run the next operation due
        """
        print "CURRENT OPERATION / OPERATIONS with client %s" % self.client
        print self._current_operation
        print self._operations
        if self._operations:
            d, f, a, kw = self._operations.pop(0)
            print "BLARGH ABOUT TO CHAIN ONTO %s" % d
            self.sequence = 0
            self._current_operation = d, f, a, kw
            self._current_operation_dfr = f(*a, **kw)
            (self._current_operation_dfr
                .addBoth(self._update_operations)
                .chainDeferred(d))
        else:
            self._current_operation_dfr = None
            self._current_operation = None
        return _result

    def _do_operation(self, func, *a, **kw):
        print "Hit do_operation in client.py"
        d = defer.Deferred()
        self._operations.append((d, func, a, kw))
        if self._current_operation_dfr is None:
            self._update_operations()
        self._last_user_dfr = d
        return d

