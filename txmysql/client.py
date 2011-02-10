from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from twisted.internet import reactor, defer
from protocol import MySQLProtocol # One instance of this per actual connection to MySQL
from txmysql import util, error

def operation(func):
    """
    Serialise the running of these functions
    """
    func = defer.inlineCallbacks(func)
    def wrap(self, *a, **kw):
        return self._do_operation(func, self, *a, **kw)
    return wrap

class MySQLConnection(ClientFactory):
    """
    Takes the responsibility for the reactor.connectTCP call away from the user.

    Lazily connects to MySQL when a query is run and stays connected only
    for up to idle_timeout seconds.

    Handles reconnecting if there are queries which have not yet had results
    delivered.

    When excuting a query, waits until query_timeout expires before giving up
    and reconnecting (assuming this MySQL connection has "gone dead"). If 
    retry_on_timeout == True, attempts the query again once reconnected.

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
            retry_on_timeout=False, temporary_error_strings=[], port=3306):

        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database
        self.connect_timeout = connect_timeout
        self.query_timeout = query_timeout
        self.idle_timeout = idle_timeout
        self.retry_on_timeout = retry_on_timeout
        self.temporary_error_strings = temporary_error_strings
        self.port = port
        self._operations = []
        self._current_operation = None
        self.deferred = defer.Deferred() # This gets called when we have a new
                                         # client which just got connected

        self.state = 'disconnected'
        self.client = None # Will become an instance of MySQLProtocol
                           # precisely when we have a live connection

    def stateTransition(self, new_state):
        print "Transition from %s to %s" % (self.state, new_state)
        self.state = new_state

    @defer.inlineCallbacks
    def _begin(self):
        print "@"*80
        print "_begin was called when we were in state %s" % self.state
        print "@"*80
        if self.state == 'disconnected':
            self.stateTransition('connecting')
            # TODO: Use UNIX socket if string is "localhost"
            # we are the factory which we pass to the reactor, is that weird?
            reactor.connectTCP(self.hostname, self.port, self)
            yield self.deferred # will set self.client
            yield self.client.ready_deferred
            self.stateTransition('connected')

    def _end(self):
        pass

    def buildProtocol(self, addr):
        print "Running buildprotocol for %s" % addr
        p = self.protocol(self.username, self.password, self.database,
                idle_timeout=self.idle_timeout)
        p.factory = self
        self.client = p
        self.deferred.callback(p)
        self.deferred = defer.Deferred()
        return p

    def clientConnectionFailed(self, connector, reason):
        print "GOT CLIENTCONNECTIONFAILED"
    
    def clientConnectionFailed(self, connector, reason):
        print "GOT CLIENTCONNECTIONLOST"

    @operation
    def runQuery(self, query): # TODO query_args
        print "Start at the beginning"
        yield self._begin()
        result = yield self.client.fetchall(query)
        yield self._end()
        defer.returnValue(result)

    @operation
    def runOperation(self, query): # TODO query_args
        yield self._begin()
        yield self.mysql_connection.client.query(query)
        yield self._end()

    # TODO: Put this code in a common base class
    def _update_operations(self, _result=None):
        print "Hit _update_operation in client.py"
        if self._operations:
            d, f, a, kw = self._operations.pop(0)
            self.sequence = 0
            self._current_operation = f(*a, **kw)
            (self._current_operation
                .addBoth(self._update_operations)
                .chainDeferred(d))
        else:
            self._current_operation = None
        return _result
    def _do_operation(self, func, *a, **kw):
        print "Hit do_operation in client.py"
        d = defer.Deferred()
        self._operations.append((d, func, a, kw))
        if self._current_operation is None:
            self._update_operations()
        return d

