import sys

from twisted.internet import reactor
from twisted.python import log

from txmysql import client
from secrets import *

import functools

log.startLogging(sys.stdout)

class ConnectionPoolTest:
    def __init__(self):
        # Create a connection pool with one connection only. When
        # another connection is requested from the pool, a
        # DeferredConnection is created. The latter is executed when a
        # Connection becomes available.
        self._pool = client.ConnectionPool(MYSQL_HOST, MYSQL_USER,MYSQL_PASS,
                                           database='test', num_connections=1,
                                           idle_timeout=120,
                                           connect_timeout=30)
    def doInsertRow(self, deferred, data, ignored):
        print "run insert deferred=%s ignored=%s" % (deferred, ignored)
        self._pool.runOperation("insert into example set data='%s'" % data)

    def doSelect(self, deferred, ignored):
        print "doing select deferred=%s ignored=%s" % (deferred, ignored)
        return self._pool.runQuery("select * from example")

    def selectTakeResults(self, deferred, data):
        print "selectTakeResults deferred=%s, data=%s" % (deferred, repr(data))

    def handleFailure(self, reason):
        # reason can be a MySQLError with message, errno, sqlstate, query
        print reason

    def stop(self, ignored):
        reactor.stop()

    def whenRunning(self):
        d = self._pool.selectDb("test")
        d.addCallback(functools.partial(self.doInsertRow, d, 'first'))
        d.addCallback(functools.partial(self.doSelect, d))
        d.addCallback(functools.partial(self.selectTakeResults, d))
        d.addErrback(self.handleFailure)

    def whenRunning2(self):
        d2 = self._pool.selectDb("test")
        d2.addCallback(functools.partial(self.doInsertRow, d2, 'second'))
        d2.addCallback(functools.partial(self.doSelect, d2))
        d2.addCallback(functools.partial(self.selectTakeResults, d2))
        d2.addCallback(self.stop)
        d2.addErrback(self.handleFailure)

if __name__ == "__main__":
    t = ConnectionPoolTest()
    reactor.callWhenRunning(t.whenRunning)
    reactor.callWhenRunning(t.whenRunning2)
    reactor.run()
