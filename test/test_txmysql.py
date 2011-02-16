"""
Test txMySQL against a local MySQL server

This test requires 'sudo' without a password, and expects a stock Ubuntu 10.04
MySQL setup. It will start and stop MySQL and occasionally replace it with an
evil daemon which absorbs packets.
"""

from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.internet.base import DelayedCall
from twisted.internet.error import ConnectionDone

DelayedCall.debug = False
from txmysql import client
from HybridUtils import AsyncExecCmds, sleep
import secrets

class MySQLClientTest(unittest.TestCase):

    def test_003_escaping(self):

        try:
            client._escape("%s", ())
            self.fail("that should have raised an exception")
        except TypeError:
            pass

        try:
            client._escape("select * from baz baz baz", (1, 2, 3))
            self.fail("that should have raised an exception")
        except TypeError:
            pass

        result = client._escape("update foo set bar=%s where baz=%s or bash=%s", ("%s", "%%s", 123))
        self.assertEquals(result, "update foo set bar='%s' where baz='%%s' or bash='123'")

    @defer.inlineCallbacks
    def test_005_test_initial_database_selection(self):
        """
        1. Start MySQL
        2. Connect
        3. Query - check result
        """
        yield self._start_mysql()
        conn = yield self._connect_mysql()
        result = yield conn.runQuery("select * from foo")
        conn.disconnect()
        self.assertEquals(result, [[1]])

    @defer.inlineCallbacks
    def test_010_start_connect_query(self):
        """
        1. Start MySQL
        2. Connect
        3. Query - check result
        """
        yield self._start_mysql()
        conn = yield self._connect_mysql()
        result = yield conn.runQuery("select 2")
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_020_stop_connect_query_start(self):
        """
        1. Connect, before MySQL is started
        2. Start MySQL
        3. Query - check result
        """
        conn = yield self._connect_mysql()
        d = conn.runQuery("select 2") # Should get connection refused, because we're not connected right now
        yield self._start_mysql()
        result = yield d
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_021_stop_connect_query_start_retry_on_error(self):
        """
        1. Connect, before MySQL is started
        2. Start MySQL
        3. Query - check result
        """
        conn = yield self._connect_mysql(retry_on_error=True)
        d = conn.runQuery("select 2") # Should get connection refused, because we're not connected right now
        yield self._start_mysql()
        result = yield d
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_030_start_idle_timeout(self):
        """
        Connect, with evildaemon in place of MySQL
        Evildaemon stops in 5 seconds, which is longer than our idle timeout
        so the idle timeout should fire, disconnecting us.
        But because we have a query due, we reconnect and get the result.
        """
        daemon_dfr = self._start_evildaemon(secs=10)
        conn = yield self._connect_mysql(idle_timeout=3, retry_on_error=True)
        d = conn.runQuery("select 2")
        yield daemon_dfr
        yield self._start_mysql()
        result = yield d
        conn.disconnect()
        self.assertEquals(result, [[2]])

    @defer.inlineCallbacks
    def test_040_start_connect_long_query_timeout(self):
        """
        Connect to the real MySQL, run a long-running query which exceeds the
        idle timeout, check that it times out and returns the appropriate
        Failure object (because we haven't set retry_on_error)
        """
        yield self._start_mysql()
        conn = yield self._connect_mysql(idle_timeout=3)
        try:
            result = yield conn.runQuery("select sleep(5)")
        except Exception, e:
            print "Caught exception %s" % e
            self.assertTrue(isinstance(e, ConnectionDone))
        finally:
            conn.disconnect()
        
    @defer.inlineCallbacks
    def test_050_retry_on_error(self):
        """
        Start a couple of queries in parallel.
        Both of them should take 10 seconds, but restart the MySQL
        server after 5 seconds.
        Setting the connection and idle timeouts allows bad connections
        to fail.
        """
        yield self._start_mysql()
        conn = yield self._connect_mysql(retry_on_error=True)
        d1 = conn.runQuery("select sleep(7)")
        d2 = conn.runQuery("select sleep(7)")
        yield sleep(2)
        yield self._stop_mysql()
        yield self._start_mysql()
        result = yield defer.DeferredList([d1, d2])
        conn.disconnect()
        self.assertEquals(result, [(True, [[0]]), (True, [[0]])])

    @defer.inlineCallbacks
    def test_055_its_just_one_thing_after_another_with_you(self):
        """
        Sanity check that you can do one thing and then another thing.
        """
        yield self._start_mysql()
        conn = yield self._connect_mysql(retry_on_error=True)
        yield conn.runQuery("select 2")
        yield conn.runQuery("select 2")
        conn.disconnect()

    @defer.inlineCallbacks
    def test_060_error_strings_test(self):
        """
        This test causes MySQL to return what we consider a temporary local
        error.  We do this by starting MySQL, querying a table, then physically
        removing MySQL's data files.

        This triggers MySQL to return a certain error code which we want to
        consider a temporary local error, which should result in a reconnection
        to MySQL.

        This is arguably the most application-specific behaviour in the txMySQL
        client library.

        """
        res = yield AsyncExecCmds([
            """sh -c '
            cd /var/lib/mysql/foo;
            chmod 0660 *;
            chown mysql:mysql *
            '"""], cmd_prefix="sudo ").getDeferred()
        yield self._start_mysql()
        conn = yield self._connect_mysql(retry_on_error=True,
            temporary_error_strings=[
                "Can't find file: './foo/foo.frm' (errno: 13)",
            ])
        yield conn.selectDb("foo")
        yield conn.runOperation("create database if not exists foo")
        yield conn.runOperation("create database if not exists foo")
        yield conn.runOperation("drop table if exists foo")
        yield conn.runOperation("create table foo (id int)")
        yield conn.runOperation("insert into foo set id=1")
        result = yield conn.runQuery("select * from foo")
        self.assertEquals(result, [[1]])

        # Now the tricky bit, we have to force MySQL to yield the error message.
        res = yield AsyncExecCmds([
            """sh -c '
            cd /var/lib/mysql/foo;
            chmod 0600 *;
            chown root:root *'
            """], cmd_prefix="sudo ").getDeferred()
        print res
        
        yield conn.runOperation("flush tables") # cause the files to get re-opened
        d = conn.runQuery("select * from foo") # This will spin until we fix the files, so do that pronto
        yield sleep(1)
        res = yield AsyncExecCmds([
            """sh -c '
            cd /var/lib/mysql/foo;
            chmod 0660 *;
            chown mysql:mysql *
            '"""], cmd_prefix="sudo ").getDeferred()
        print res
        result = yield d
        self.assertEquals(result, [[1]])
        conn.disconnect()
    
    # Utility functions:

    def _stop_mysql(self):
        return AsyncExecCmds(['stop mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_mysql(self):
        return AsyncExecCmds(['start mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_evildaemon(self, secs):
        """
        Simulates a MySQL server which accepts connections but has mysteriously
        stopped returning responses at all, i.e. it's just /dev/null
        """
        return AsyncExecCmds(['python ../test/evildaemon.py %s' % str(secs)], cmd_prefix='sudo ').getDeferred()
    
    def setUp(self):
        """
        Stop MySQL before each test
        """
        return self._stop_mysql()

    def tearDown(self):
        """
        Stop MySQL before each test
        """
        reactor.disconnectAll()

    def _connect_mysql(self, **kw):
        return client.MySQLConnection('127.0.0.1', 'root', secrets.MYSQL_ROOT_PASS, 'foo', **kw)
