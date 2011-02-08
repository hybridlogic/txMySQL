from txmysql.protocol import MySQLProtocol, MySQLClientFactory
from twisted.internet import defer
from twisted.application.internet import UNIXClient
from twisted.internet import reactor
from twisted.application.service import Application
from twisted.protocols import policies
import pprint
import secrets

factory = MySQLClientFactory(username='root', password=secrets.MYSQL_ROOT_PASS, database='mysql')

class TestProtocol(MySQLProtocol):
    def __init__(self, *args, **kw):
        MySQLProtocol.__init__(self, *args, **kw)

    def connectionMade(self):
        MySQLProtocol.connectionMade(self)
        self.do_test()

    def connectionLost(self, reason):
        print reason
    def connectionFailed(self, reason):
        print reason

    @defer.inlineCallbacks
    def do_test(self):
        yield self.ready_deferred
        yield self.select_db('foo')

        result = yield self.runQuery('select * from bar')
        print result

factory.protocol = TestProtocol
#factory = policies.SpewingFactory(factory)
reactor.connectTCP('127.0.0.1', 3306, factory)

application = Application("Telnet Echo Server")

