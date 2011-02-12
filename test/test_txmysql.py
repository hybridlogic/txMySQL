"""
Test txMySQL against a local MySQL server

This test requires 'sudo' without a password, and expects a stock Ubuntu 10.04
MySQL setup. It will start and stop MySQL and occasionally replace it with an evil daemon.
"""

from twisted.trial import unittest
from twisted.internet import defer

from AwesomeProxy import AwesomeProxy
from proxies.SMTPProxy import SMTPProxy

class AwesomeProxyTest(unittest.TestCase):

    @defer.inlineCallbacks
    def test_010_start_connect_query(self):
        """
        1. Start MySQL
        2. Connect
        3. Query - check result
        """
        pass

    @defer.inlineCallbacks
    def test_020_stop_connect_query_start(self):
        """
        1. Connect, before MySQL is started
        1. Start MySQL
        3. Query - check result
        """
        pass

    @defer.inlineCallbacks
    def test_030_start_connect_timeout(self):
        """

        ... Check that MySQL still functions with a normal query afterwards
        """
        pass

    @defer.inlineCallbacks
    def test_040_start_connect_long_query_timeout(self):
        pass

    @defer.inlineCallbacks
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

    @defer.inlineCallbacks
    def test_060_no_retry_on_error_start_connect_query_restart(self):
        pass

    @defer.inlineCallbacks
    def test_070_error_strings_test(self):
        pass

    @defer.inlineCallbacks
    def test_080_retry_on_error_start_connect_query_restart(self):
        pass

    @defer.inlineCallbacks
    def test_090_no_retry_on_error_start_connect_query_restart(self):
        pass

    @defer.inlineCallbacks
    def test_100_evil_daemon_timeout(self):
        """
        1. Start evil daemon
        2. Connect with idle_timeout=5
        3. Connection should time out
        """
    
    # Utility functions:

    def _stop_mysql(self):
        return AsyncExecCmds(['pkill -9 mysqld; sudo stop mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_mysql(self):
        return AsyncExecCmds(['start mysql'], cmd_prefix='sudo ').getDeferred()
    
    def _start_evildaemon(self):
        """
        Simulates a MySQL server which accepts connections but has mysteriously stopped
        returning responses at all, i.e. it's just /dev/null
        """
        return AsyncExecCmds(['sudo python test/evildaemon.py'], cmd_prefix='sudo ').getDeferred()
    
    def setUp(self):
        """
        Stop MySQL before each test
        """
        return self._stop_mysql()
