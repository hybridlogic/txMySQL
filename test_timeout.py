from txmysql.protocol import MySQLConnection, error
from twisted.internet import reactor, defer
import secrets
from twisted.application.service import Application
import time
import random
from HybridUtils import AsyncExecCmds, sleep

"""

Simulate the worst possible scenario for connectivity to a MySQL server

The server randomly disappears and re-appears, and is sometimes replaced by
an imposter who accepts packets but never responds.

This is sorta like being on a train between Bristol and London on O2's 3G
network.

"""

conn = MySQLConnection('127.0.0.1', 'root', secrets.MYSQL_ROOT_PASS, 'foo',
        connect_timeout=5, query_timeout=5, idle_timeout=5, retry_on_timeout=True)

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
def main():
    #fuck_with_mysql_server()
    while 1:
        # pick a random value which may or may not trigger query timeout
        # remember, the mysql server only stays up for 
        wait = random.randrange(3, 7)
        print "About to yield on select sleep(%i)" % wait
        result = yield conn.runQuery("select sleep(%i)" % wait)
        print "============================================================="
        print "THIS RESULT IS SACRED AND SHOULD ALWAYS BE RETURNED CORRECTLY"
        print result
        print "============================================================="

reactor.callLater(0, main)

application = Application("Evil MySQL reconnection tester")


