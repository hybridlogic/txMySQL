from twisted.internet import reactor, protocol
import random

class Echo(protocol.Protocol):
    def dataReceived(self, data):
        pass

def main():
    """This runs the protocol on port 8000"""
    factory = protocol.ServerFactory()
    factory.protocol = Echo
    reactor.listenTCP(3306, factory)
    reactor.callLater(random.randrange(3,15), reactor.stop)
    reactor.run()

if __name__ == '__main__':
    main()
