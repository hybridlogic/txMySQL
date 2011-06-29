from twisted.internet import defer
from twisted.protocols.policies import TimeoutMixin
from twisted.python import log
from qbuf.twisted_support import MultiBufferer, MODE_STATEFUL
from twisted.internet.protocol import Protocol
from txmysql import util, error
import struct
from hashlib import sha1
import sys
#import pprint
import datetime

typemap = {
    0x01: 1,
    0x02: 2,
    0x03: 4,
    0x04: 4,
    0x05: 8,
    0x08: 8,
    0x09: 3,
    0x0c: 8
}

def _xor(message1, message2):
	length = len(message1)
	result = ''
	for i in xrange(length):
		x = (struct.unpack('B', message1[i:i+1])[0] ^ struct.unpack('B', message2[i:i+1])[0])
		result += struct.pack('B', x)
	return result

def dump_packet(data):
	def is_ascii(data):
		if data.isalnum():
			return data
		return '.'
	print "packet length %d" % len(data)
	print "method call: %s \npacket dump" % sys._getframe(2).f_code.co_name
	print "-" * 88
	dump_data = [data[i:i+16] for i in xrange(len(data)) if i%16 == 0]
	for d in dump_data:
		print ' '.join(map(lambda x:"%02X" % ord(x), d)) + \
				'   ' * (16 - len(d)) + ' ' * 2 + ' '.join(map(lambda x:"%s" % is_ascii(x), d))
	print "-" * 88
	print ""

def operation(func):
    func = defer.inlineCallbacks(func)
    def wrap(self, *a, **kw):
        return self._do_operation(func, self, *a, **kw)
    return wrap

class MySQLProtocol(MultiBufferer, TimeoutMixin):
    mode = MODE_STATEFUL
    def getInitialState(self):
        return self._read_header, 4
    
    def timeoutConnection(self):
        #print "Timing out connection"
        TimeoutMixin.timeoutConnection(self)

    def connectionClosed(self, reason):
        #print "CONNECTIONCLOSED"
        #MultiBufferer.connectionClosed(self, reason)
        Protocol.connectionClosed(self, reason)
    
    def connectionLost(self, reason):
        #print "CONNECTIONLOST"
        #MultiBufferer.connectionLost(self, reason)
        # XXX When we call MultiBufferer.connectionLost, we get
        # unhandled errors (something isn't adding an Errback
        # to the deferred which eventually gets GC'd, but I'm
        # not *too* worried because it *does* get GC'd).
        # Do check that the things which yield on read() on
        # the multibufferer get correctly GC'd though.
        Protocol.connectionLost(self, reason)
    
    def _read_header(self, data):
        length, _ = struct.unpack('<3sB', data)
        length = util.unpack_little_endian(length)
        def cb(data):
            log.msg('unhandled packet: %r' % (data,))
            return self._read_header, 4
        return cb, length

    def __init__(self, username, password, database, idle_timeout=None):
        MultiBufferer.__init__(self)
        self.username, self.password, self.database = username, password, database
        self.sequence = None
        self.ready_deferred = defer.Deferred()
        self._operations = []
        self._current_operation = None
        self.factory = None
        self.setTimeout(idle_timeout)
        self.debug_query = None

    @defer.inlineCallbacks
    def read_header(self):
        length, seq = yield self.unpack('<3sB')
        self.sequence = seq + 1
        length = util.unpack_little_endian(length)
        defer.returnValue(util.LengthTracker(self, length))

    @defer.inlineCallbacks
    def read_eof(self, read_header=True):
        ret = {'is_eof': True}
        if read_header:
            t = yield self.read_header()
            yield t.read(1)
            unpacker = t.unpack
        else:
            unpacker = self.unpack
        ret['warning_count'], ret['flags'] = yield unpacker('<HH')
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def read_rows(self, columns):
        ret = []
        while True:
            t = yield self.read_header()
            length = yield t.read_lcb()
            if length is util._EOF:
                break
            x = columns
            row = []
            while True:
                if length is None:
                    row.append(None)
                else:
                    row.append((yield t.read(length)))
                x -= 1
                if not x:
                    break
                length = yield t.read_lcb()
            row.append((yield t.read_rest()))
            ret.append(row)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def read_result(self, is_prepare=False, read_rows=True, data_types=None):
        self.resetTimeout()
        t = yield self.read_header()
        field_count = yield t.read_lcb()
        ret = {}
        if field_count == util._EOF:
            ret = yield self.read_eof(read_header=False)
        elif field_count == 0:
            if is_prepare:
                ret['stmt_id'], ret['columns'], ret['parameters'], ret['warning_count'] = yield t.unpack('<IHHxH')
                if ret['parameters']:
                    ret['placeholders'] = yield self.read_fields()
                ret['fields'] = yield self.read_fields()
            elif data_types is not None:
                nulls = yield t.read((len(data_types) + 7) // 8)
                nulls = util.unpack_little_endian(nulls) >> 2
                cols = []
                for type in data_types:
                    if nulls & 1:
                        cols.append(None)
                    elif type in typemap:
                        val = yield t.read(typemap[type])
                        if type == 4:
                            val, = struct.unpack('<f', val)
                        elif type == 5:
                            val, = struct.unpack('<d', val)
                        elif type == 12:
                            try:
                                val = datetime.datetime(*struct.unpack("<xHBBBBB",val)).strftime('%Y-%m-%d %H:%M:%S')
                            except Exception, e:
                                print "CRITICAL: Caught exception in txmysql when trying",
                                print "to decode datetime value %r" % (val,),
                                print "exception follows, returning None to application",
                                print e
                                val = None
                        else:
                            val = util.unpack_little_endian(val)
                        cols.append(val)
                    else:
                        cols.append((yield t.read_lcs()))
                    nulls >>= 1
                ret['cols'] = cols
                ret['x'] = yield t.read_rest()
            else:
                ret['affected_rows'] = yield t.read_lcb()
                ret['insert_id'] = yield t.read_lcb()
                ret['server_status'], ret['warning_count'] = yield t.unpack('<HH')
                ret['message'] = yield t.read_rest()
        elif field_count == 0xff:
            errno, sqlstate = yield t.unpack('<Hx5s')
            message = yield t.read_rest()
            raise error.MySQLError(message, errno, sqlstate, self.debug_query)
        else:
            if t:
                ret['extra'] = yield t.read_lcb()
            ret['fields'] = yield self.read_fields()
            if read_rows:
                ret['rows'] = yield self.read_rows(field_count)
                ret['eof'] = yield self.read_eof(read_header=False)

        defer.returnValue(ret)

    @defer.inlineCallbacks
    def read_fields(self):
        fields = []
        while True:
            field = yield self.read_field()
            if field is util._EOF:
                yield self.read_eof(read_header=False)
                break
            fields.append(field)
        defer.returnValue(fields)

    @defer.inlineCallbacks
    def read_field(self):
        t = yield self.read_header()
        ret = {}
        first_length = yield t.read_lcb()
        if first_length is util._EOF:
            defer.returnValue(util._EOF)
        ret['catalog'] = yield t.read(first_length)
        for name in ['db', 'table', 'org_table', 'name', 'org_name']:
            ret[name] = yield t.read_lcs()
        ret['charsetnr'], ret['length'], ret['type'] = yield t.unpack('<xHIB')
        ret['flags'], ret['decimals'] = yield t.unpack('<HBxx')
        if t:
            ret['default'] = yield t.read_lcb()
        defer.returnValue(ret)

    def connectionMade(self):
        d = self.do_handshake()
        def done_handshake(data):
            self.ready_deferred.callback(data) # Handles errbacks too
            self.ready_deferred = defer.Deferred()
        d.addBoth(done_handshake)
    
    def _update_operations(self, _result=None):
        if self._operations:
            d, f, a, kw = self._operations.pop(0)
            self.sequence = 0
            self._current_operation = f(*a, **kw)
            (self._current_operation
                .addBoth(self._update_operations)
                .chainDeferred(d))
        else:
            self._current_operation = None
        return _result
    
    def _do_operation(self, func, *a, **kw):
        d = defer.Deferred()
        self._operations.append((d, func, a, kw))
        if self._current_operation is None:
            self._update_operations()
        return d

    @operation
    def do_handshake(self):
        self.resetTimeout()
        t = yield self.read_header()
        protocol_version, = yield t.unpack('<B')
        yield t.read_cstring() # server_version
        thread_id, scramble_buf = yield t.unpack('<I8sx')
        capabilities, language, status = yield t.unpack('<HBH')
        #print hex(capabilities)
        capabilities ^= capabilities & 32
        capabilities |= 0x30000
        if self.database:
            capabilities |= 1 << 3 # CLIENT_CONNECT_WITH_DB
        yield t.read(13)
        scramble_buf += yield t.read(12) # The last byte is a NUL
        yield t.read(1)

        scramble_response = _xor(sha1(scramble_buf+sha1(sha1(self.password).digest()).digest()).digest(), sha1(self.password).digest())

        with util.DataPacker(self) as p:
            p.pack('<IIB23x', capabilities, 2**23, language)
            p.write_cstring(self.username)
            p.write_lcs(scramble_response)
            if self.database:
                p.write_cstring(self.database)

        result = yield self.read_result()
        defer.returnValue(result)
    
    @operation
    def _ping(self):
        with util.DataPacker(self) as p:
            p.write('\x0e')
        
        result = yield self.read_result()
        import pprint; pprint.pprint(result)
    
    @operation
    def _prepare(self, query):
        with util.DataPacker(self) as p:
            p.write('\x16')
            p.write(query)
        
        result = yield self.read_result(is_prepare=True)
        defer.returnValue(result)
    
    @operation
    def _execute(self, stmt_id):
        with util.DataPacker(self) as p:
            p.pack('<BIBIB', 0x17, stmt_id, 1, 1, 1)
        result = yield self.read_result(read_rows=False)
        defer.returnValue([d['type'] for d in result['fields']])
  

    @operation
    def _fetch(self, stmt_id, rows, types):
        with util.DataPacker(self) as p:
            p.pack('<BII', 0x1c, stmt_id, rows)
        
        rows = []
        while True:
            result = yield self.read_result(data_types=types)
            if result.get('is_eof'):
                more_rows = not result['flags'] & 128
                break
            rows.append(result)

        defer.returnValue((rows, more_rows))


    @operation
    def select_db(self, database):
        with util.DataPacker(self) as p:
            p.write('\x02')
            p.write(database)
        
        result = yield self.read_result()

    
    @operation
    def query(self, query):
        "A query with no response data"
        self.debug_query = query
        with util.DataPacker(self) as p:
            p.write('\x03')
            p.write(query)

        ret = yield self.read_result()
        defer.returnValue(ret)
    
    @operation
    def _close_stmt(self, stmt_id):
        """
        Destroy a prepared statement. The statement handle becomes invalid.
        """
        with util.DataPacker(self) as p:
            p.pack('<BI', 0x19, stmt_id)
        yield defer.succeed(True)


    @defer.inlineCallbacks
    def fetchall(self, query):
        #assert '\0' not in query, 'No NULs in your query, boy!'
        self.debug_query = query
        result = yield self._prepare(query)
        types = yield self._execute(result['stmt_id'])

        all_rows = []
        while True:
            rows, more_rows = yield self._fetch(result['stmt_id'], 2, types)
            for row in rows:
                all_rows.append(row['cols'])
            if not more_rows:
                break
        #print "****************************** Got last result" 
        yield self._close_stmt(result['stmt_id'])
        defer.returnValue(all_rows)
