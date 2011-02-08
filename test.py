from txmysql.protocol import MysqlProtocol
from twisted.internet import defer
from twisted.internet.protocol import ClientFactory
from twisted.application.internet import UNIXClient
from twisted.application.service import Application
from twisted.protocols import policies

factory = ClientFactory()

class TestProtocol(MysqlProtocol):
    def connectionMade(self):
        MysqlProtocol.connectionMade(self)
        self.do_test()
    
    @defer.inlineCallbacks
    def do_test(self):
        yield self.ready_deferred
        yield self.select_db('test')
        yield self.do_query('select 1; select 2')
        
        return
        result = yield self.prepare('select * from item_db')
        types = yield self.execute(result['stmt_id'])
        while True:
            rows, more_rows = yield self.fetch(result['stmt_id'], 2, types)
            import pprint; pprint.pprint((rows, more_rows))
            if not more_rows:
                break

factory.protocol = TestProtocol
factory = policies.SpewingFactory(factory)
service = UNIXClient('/tmp/mysql.sock', factory)

application = Application("Telnet Echo Server")
service.setServiceParent(application)
