from txmysql.protocol import MySQLClientFactory, MySQLProtocol, error
from twisted.internet import reactor, defer
import secrets
from twisted.application.service import Application

factory = MySQLClientFactory(username='root', password=secrets.MYSQL_ROOT_PASS)

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
        try:
            yield self.query("drop table testing")
        except error.MySQLError, e:
            print "Table doesn't exist, ignoring %s" % str(e)

        yield self.query("""create table testing (
            id int primary key auto_increment,
            strings varchar(255),
            numbers int)""")
        
        results = yield self.fetchall("select * from testing")
        print results # should be []

        for i in range(10):
            yield self.query("insert into testing set strings='Hello world', numbers=%i" % i)
        results = yield self.fetchall("select * from testing")
        print results

factory.protocol = TestProtocol
#factory = policies.SpewingFactory(factory)
reactor.connectTCP('127.0.0.1', 3306, factory)

application = Application("Telnet Echo Server")

