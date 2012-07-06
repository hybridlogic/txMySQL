from twisted.internet import defer
import cStringIO
import struct

_EOF = object()
_lcb_lengths = {
    252: 2,
    253: 3,
    254: 8,
}

def unpack_little_endian(s):
    ret = 0
    for c in reversed(s):
        ret = (ret << 8) | ord(c)
    return ret

def pack_little_endian(v, pad_to=None):
    ret = ''
    while v:
        ret += chr(v & 0xff)
        v >>= 8
    if pad_to is not None:
        ret = ret.ljust(pad_to, '\0')
    return ret

class LengthTracker(object):
    def __init__(self, wrap, length):
        self.length = 0
        self.tot_length = length
        self.w = wrap

    def _check_length(self):
        if self.length > self.tot_length:
            import pprint; pprint.pprint(vars(self))
            raise ValueError('reading beyond the length of the packet')

    def read(self, length):
        if not length:
            return defer.succeed('')
        self.length += length
        self._check_length()
        return self.w.read(length)

    def read_remain(self):
        if self.length == self.tot_length:
            return defer.succeed('')
        return self.w.read(self.tot_length-self.length)

    @defer.inlineCallbacks
    def readline(self, delimiter=None):
        line = yield self.w.readline(delimiter)
        self.length += len(line)+1 # 1 is the length of delimiter
        self._check_length()
        defer.returnValue(line)

    def unpack(self, fmt):
        self.length += struct.calcsize(fmt)
        self._check_length()
        return self.w.unpack(fmt)

    def read_cstring(self):
        return self.readline(delimiter='\0')

    @defer.inlineCallbacks
    def read_lcb(self):
        val, = yield self.unpack('<B')
        if val == 0xfe and self.tot_length < 9:
            defer.returnValue(_EOF)
        if val <= 250 or val == 255:
            defer.returnValue(val)
        elif val == 251:
            defer.returnValue(None)
        bytes = yield self.read(_lcb_lengths[val])
        defer.returnValue(unpack_little_endian(bytes))

    @defer.inlineCallbacks
    def read_lcs(self):
        length = yield self.read_lcb()
        defer.returnValue((yield self.read(length)))

    def read_rest(self):
        return self.read(self.tot_length - self.length)
    
    def __nonzero__(self):
        return self.length < self.tot_length

class DataPacker(object):
    def __init__(self, wrap):
        self.io = cStringIO.StringIO()
        self.w = wrap

    def pack(self, fmt, *values):
        self.io.write(struct.pack(fmt, *values))

    def write(self, data):
        self.io.write(data)

    def write_cstring(self, data):
        self.io.write(data)
        self.io.write('\0')

    def write_lcb(self, value):
        if value < 0:
            raise ValueError('write_lcb packs bytes 0 <= x <= 2**64 - 1')
        elif value <= 250:
            self.io.write(chr(value))
            return
        elif value <= 2**16 - 1:
            self.io.write('\xfc')
            pad = 2
        elif value <= 2**24 - 1:
            self.io.write('\xfd')
            pad = 3
        elif value <= 2**64 - 1:
            self.io.write('\xfe')
            pad = 8
        else:
            raise ValueError('write_lcb packs bytes 0 <= x <= 2**64 - 1')
        self.io.write(pack_little_endian(value, pad))

    def write_lcs(self, string):
        self.write_lcb(len(string))
        self.write(string)

    def as_header(self, number):
        length = pack_little_endian(self.io.tell(), 3)
        return length + chr(number)

    def get_value(self):
        return self.io.getvalue()

    def to_transport(self, transport, number):
        transport.write(self.as_header(number))
        transport.write(self.get_value())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            return
        self.to_transport(self.w.transport, self.w.sequence)
        self.w.sequence += 1
