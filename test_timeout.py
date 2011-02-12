from txmysql.client import MySQLConnection, error
from twisted.internet import reactor, defer
import secrets
from twisted.application.service import Application
import time
import random
import shutil
import pprint
from HybridUtils import AsyncExecCmds, sleep
from twisted.python import log

defer.setDebugging(True)
"""

Simulate the worst possible scenario for connectivity to a MySQL server

The server randomly disappears and re-appears, and is sometimes replaced by
an imposter who accepts packets but never responds.

This is sorta like being on a train between Bristol and London on O2's 3G
network.

"""

conn = MySQLConnection('127.0.0.1', 'root', secrets.MYSQL_ROOT_PASS, 'foo',
        connect_timeout=5, query_timeout=5, idle_timeout=5, retry_on_error=True)

@defer.inlineCallbacks
def fuck_with_mysql_server():
    print "Stopping MySQL"
    AsyncExecCmds(['pkill -9 mysqld; stop mysql'], cmd_prefix='sudo ').getDeferred()
    # The pkill -9 mysqld causes "Lost connection to MySQL server during query"
    # or "MySQL server has gone away" if you try to query on a connection which has died
    while 1:
        if random.choice([0,1]) == 0:
            # Only sometimes will the MySQL server be up for long enough to
            # successfully return a SELECT
            print "Starting MySQL"
            yield AsyncExecCmds(['start mysql'], cmd_prefix='sudo ').getDeferred()
            wait = random.randrange(3, 10)
            yield sleep(wait)
            print "Stopping MySQL"
            yield AsyncExecCmds(['pkill -9 mysqld; stop mysql'], cmd_prefix='sudo ').getDeferred()
            wait = random.randrange(1, 5)
            yield sleep(wait)
        else:
            # And sometimes MySQL will be replaced with an evil daemon
            # which accepts TCP connections for 10-20 seconds but stays silent
            # This causes the official MySQL client to say:
            # Lost connection to MySQL server at 'reading initial communication packet', system error: 0
            # ... when the connection finally dies (i.e. when evildaemon.py stops)
            print "Starting evil daemon"
            yield AsyncExecCmds(['python test/evildaemon.py'], cmd_prefix='sudo ').getDeferred()
            print "Evil daemon stopped"
            wait = random.randrange(1, 5)
            yield sleep(wait)

@defer.inlineCallbacks
def render_status():
    while 1:
        fp = open("status.tmp", "w")
        yield sleep(0.1)
        fp.write("Operations:\n\n")
        fp.write(pprint.pformat(conn._pending_operations) + "\n\n")
        fp.write("Current operation;\n\n")
        fp.write(pprint.pformat(conn._current_operation) + "\n\n")
        fp.write("Current operation deferred:\n\n")
        fp.write(pprint.pformat(conn._current_operation_dfr) + "\n\n")
        fp.write("Current user deferred:\n\n")
        fp.write(pprint.pformat(conn._current_user_dfr) + "\n\n")
        fp.close()
        shutil.move("status.tmp", "status.txt")

@defer.inlineCallbacks
def main():
    fuck_with_mysql_server()
    render_status()
    while 1:
        # pick a random value which may or may not trigger query timeout
        # remember, the mysql server only stays up for a short while
        wait = random.randrange(3, 7)
        print "About to yield on select sleep(%i)" % wait
        try:
            d1 = conn.runQuery("select sleep(%i)" % wait)
            d2 = conn.runQuery("select sleep(%i)" % (wait + 1))
            d3 = conn.runQuery("select sleep(%i)" % (wait + 2))
            print "============================================================="
            print "I have been promised a result on %s" % str([d1,d2,d3])
            print "============================================================="
            d = defer.DeferredList([d1, d2, d3])
            result = yield d
            print "============================================================="
            print "THIS RESULT IS SACRED AND SHOULD ALWAYS BE RETURNED CORRECTLY %s" % str(d)
            print result
            print "============================================================="
            print "about to go under..."
            yield sleep(random.randrange(3, 7))
        except Exception, e:
            print "AAAAAAAAAAAAAARGH I GOT A FAILURE AS AN EXCEPTION"
            print e
            print "AAAAAAAAAAAAAARGH I GOT A FAILURE AS AN EXCEPTION, sleeping..."
            yield sleep(1)

reactor.callLater(0, main)

application = Application("Evil MySQL reconnection tester")


