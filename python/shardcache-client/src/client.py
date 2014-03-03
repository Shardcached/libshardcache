##
## SHARDCACHE.PY
##
###############

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

    def get(self, key):
        self._send_message(message = MSG_GET,
                           records = [key])

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
        data = self.socket.recv(1024)
        print 'x'
        print repr(data)


if __name__ == '__main__':
    shard = ShardcacheClient('ln.xant.net', 4443, 'default')
    shard.get('b.o.txt')

