from twisted.internet import reactor, protocol
import random
import sys

class Echo(protocol.Protocol):
    def dataReceived(self, data):
        pass

def main():
    factory = protocol.ServerFactory()
    factory.protocol = Echo
    reactor.listenTCP(3306, factory)
    reactor.callLater(int(sys.argv[1]), reactor.stop)
    reactor.run()

if __name__ == '__main__':
    main()
