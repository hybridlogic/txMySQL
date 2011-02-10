from twisted.internet.protocol import ClientFactory, ReconnectingClientFactory
from protocol import MySQLProtocol # One instance of this per actual connection to MySQL

def operation(func):
    """
    Serialise the running of this function
    Note that the version in this file does NOT automatically add
    inlineCallbacks.
    """
    def wrap(self, *a, **kw):
        return self._do_operation(func, self, *a, **kw)
    return wrap

class MySQLConnection(ClientFactory):

    def __init__(self):
        self.clientProtocol = None
        self._operations = []
        self.connected = False

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

    @operation
    def runOperation(self):
        pass

    @operation
    def runQuery(self):
        pass
