"""
Test txMySQL against a local MySQL server

This test requires 'sudo' without a password, and expects a stock Ubuntu 10.04
MySQL setup. It will start and stop MySQL and occasionally replace it with an
evil daemon which absorbs packets.
"""

from twisted.trial import unittest
from twisted.internet import defer, reactor
from twisted.internet.base import DelayedCall

DelayedCall.debug = True
from txmysql import client
from HybridUtils import AsyncExecCmds, sleep
import secrets

class AwesomeProxyTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_010_start_connect_query(self):
        return
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
        return
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
        return
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
    def test_030_start_connect_timeout(self):
        return
        """
        Connect, with evildaemon in place of MySQL
        Evildaemon stops in 5 seconds, which is longer than our idle timeout
        so the idle timeout should fire, disconnecting us.
        But because we have a query due, we reconnect.
        """
        daemon_dfr = self._start_evildaemon(secs=10) # Do not yield, it is synchronous
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
            raise e
        finally:
            conn.disconnect()

    #@defer.inlineCallbacks
    def test_050_retry_on_error(self):
        """
        1. Start MySQL
        2. Connect
        3. Run a long query
        4. Restart MySQL before the long query executes
        5. Check that MySQL reconnects and eventually returns the
           correct result
        """
        pass

    #@defer.inlineCallbacks
    def test_060_no_retry_on_error_start_connect_query_restart(self):
        pass

    #@defer.inlineCallbacks
    def test_070_error_strings_test(self):
        pass

    #@defer.inlineCallbacks
    def test_080_retry_on_error_start_connect_query_restart(self):
        pass

    #@defer.inlineCallbacks
    def test_090_no_retry_on_error_start_connect_query_restart(self):
        pass

    #@defer.inlineCallbacks
    def test_100_evil_daemon_timeout(self):
        """
        1. Start evil daemon
        2. Connect with idle_timeout=5
        3. Connection should time out
        """
        pass
    
    # Utility functions:

    def _stop_mysql(self):
        return AsyncExecCmds(['stop mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_mysql(self):
        return AsyncExecCmds(['start mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_evildaemon(self, secs):
        """
        Simulates a MySQL server which accepts connections but has mysteriously stopped
        returning responses at all, i.e. it's just /dev/null
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
