##
## SHARDCACHE.PY
##
###############

import select
import socket
import struct
from siphash import SipHash

MSG_GET = 0x01
MSG_SET = 0x02
MSG_DEL = 0x03
MSG_EVI = 0x04
MSG_OFX = 0x06
MSG_ADD = 0x07
MSG_EXI = 0x08

MSG_CHK = 0x31
MSG_STS = 0x32

RECORD_SEPARATOR = 0x80
RECORD_TERMINATOR = (0x00, 0x00)

def chunkize(string):
    while len(string) > 0xffff:
        yield struct.pack('!H', len(string))
        yield string[:0xffff]
        string = string[0xffff:]

    if len(string):
        yield struct.pack('!H', len(string))
        yield string

    yield chr(0)
    yield chr(0)

class ShardcacheClient:
    "Simple Python client for shardcache"

    def __init__(self, host, port, secret):
        secret = secret[:16]
        secret += chr(0) * (16 - len(secret))
        self.secret = struct.pack('16c', *secret)
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        self.input_buffer = []

    def get(self, key):
        records = self._send_message(message = MSG_GET,
                                     records = [key])
        return ''.join(records[0])
  

    def set(self, key, value):
        self._send_message(message = MSG_SET,
                           records = [key, value])

    def delete(self, key):
        self.__send_message(message = MSG_DEL,
                            records = [key])

    def _send_message(self, message, records=None):
        # request
        packet = []
        packet.append(chr(message))

        if records:
            for r in records:
                if len(packet) > 1 :
                    packet.append(0x80)
                packet += list(chunkize(r))
        else :
            packet.extend(RECORD_TERMINATOR);

        packet.append(chr(0))


        # signing
        packetbuf = ''.join(packet)

        print repr(self.secret), len(self.secret)
        print 'x'
        print repr(packetbuf), len(packetbuf)
        siphash = SipHash(c=2, d=4)
        signature = siphash.auth(struct.unpack('<QQ', self.secret)[0], packetbuf)

        packetbuf = ''.join((chr(0x73), chr(0x68), chr(0x63), chr(0x01), chr(0xF0))) + packetbuf + struct.pack('<Q', signature);

        print 'packet', repr(packetbuf)

        # response
        self.socket.sendall(packetbuf)
        self.socket.setblocking(0)

        retcords = None

        readable, writable, exceptional = select.select([self.socket], [], [], 0.5)
        while readable:
            if readable[0] == self.socket:
                data = self.socket.recv(1024)
                records = self._process_input(data)
                if records:
                    break
            readable = select.select([self.socket], [], [], 0.5)
            print readable
            if exceptional and exceptional[0] == self.socket:
                print >>sys.stderr, 'handling exceptional condition for', s.getpeername()
                break

        return records


    def _process_input(self, data):
        self.input_buffer.extend(data);
        if len(self.input_buffer) < 8:
            return None

        if data[:3] != 'shc':
            print "Bad magic " + repr(data[:3])

        offset = 3

        pversion = data[offset]
        offset += 1

        signed = ''

        header = data[offset]
        offset += 1

        if header == '\xf0' or header == '\xf1':
            signed = header
            header = data[offset]
            offset += 1

        records = []
        while True:
            chunk_size = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2

            record = []
            while chunk_size:
                if len(data) < offset + chunk_size:
                    return None
                record.extend(data[offset:offset+chunk_size])
                offset += chunk_size
                chunk_size = struct.unpack('>H', data[offset:offset+2])[0]
                offset += 2

            records.append(record)

            sep = data[offset]
            offset += 1
            if sep == '\x00':
                break
            if sep != '\x80':
                print >>sys.stderr, 'Bad separator ', sep, 'from ', s.getpeername()


        if signed:
            signature = data[offset:offset + 8]
            offset += 8

        # we have a complete record let's send it back and flush the input accumulator
        self.input_buffer = self.input_buffer[offset:]

        return records


if __name__ == '__main__':
    shard = ShardcacheClient('ln.xant.net', 4443, 'default')
    print shard.get('b.o.txt')

