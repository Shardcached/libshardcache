##
## SHARDCACHE.PY
##
###############

import select
import socket
import struct
import sys
from siphash import SipHash

MSG_GET = chr(0x01)
MSG_SET = chr(0x02)
MSG_DEL = chr(0x03)
MSG_EVI = chr(0x04)
MSG_OFX = chr(0x06)
MSG_ADD = chr(0x07)
MSG_EXI = chr(0x08)

MSG_CHK = chr(0x31)
MSG_STS = chr(0x32)

MSG_SIG  = chr(0xF0)
MSG_CSIG = chr(0xF1)

RES_OK     = chr(0x00)
RES_YES    = chr(0x01)
RES_EXISTS = chr(0x02)
RES_NO     = chr(0xFE)
RES_ERR    = chr(0xFF)


MESSAGE_TERMINATOR = chr(0x00)
RECORD_SEPARATOR   = chr(0x80)
RECORD_TERMINATOR  = (chr(0x00), chr(0x00))

def chunkize(buf):
    while len(buf) > 0xffff:
        yield struct.pack('!H', len(buf))
        yield buf[:0xffff]
        buf = buf[0xffff:]

    if len(buf):
        yield struct.pack('!H', len(buf))
        yield buf

    yield chr(0)
    yield chr(0)

class ShardcacheClient:
    "Simple Python client for shardcache"

    def __init__(self, host, port, secret=None):
        if secret:
            secret = secret[:16]
            secret += chr(0) * (16 - len(secret))
            self.secret = struct.pack('16c', *secret)
        else:
            self.secret = None
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((host, port))
        self.input_buffer = []

    def get(self, key):
        records = self._send_message(message = MSG_GET,
                                     records = [key])
        if records:
            return ''.join(records[0])

        return None
  
    def sts(self):
        records = self._send_message(message = MSG_STS)
        if records:
            return ''.join(records[0])

        return None
 
    def set(self, key, value):
        records = self._send_message(message = MSG_SET,
                                     records = [key, value])

        if records and ord(records[0]) == RES_OK:
            return 0;

        return -1

    def delete(self, key):
        records = self.__send_message(message = MSG_DEL,
                                      records = [key])

        if records and ord(records[0]) == RES_OK:
            return 0;

        return -1

    def _send_message(self, message, records=None):
        # request
        packet = []
        packet.append(message)

        if records:
            for r in records:
                if len(packet) > 1 :
                    packet.append(RECORD_SEPARATOR)
                packet += list(chunkize(r))
        else :
            packet.extend(RECORD_TERMINATOR);

        packet.append(chr(0))


        # signing
        content = ''.join(packet)

        #print repr(self.secret), len(self.secret)
        #print repr(packetbuf), len(packetbuf)
        siphash = SipHash(c=2, d=4)
        signature = siphash.auth(struct.unpack('<QQ', self.secret)[0], content)

        packetbuf = 'shc\x01'
        if self.secret: 
            packetbuf += MSG_SIG + content + struct.pack('<Q', signature)
        else:
            packetbuf += content

        #print 'packet', repr(packetbuf)

        self.socket.setblocking(1)
        self.socket.sendall(packetbuf)
        self.socket.setblocking(0)

        # response
        retcords = None
        # read until we have a full message
        readable, writable, exceptional = select.select([self.socket], [], [], 0.5)
        while readable:
            if readable[0] == self.socket:
                data = self.socket.recv(1024)
                # _process_input() will returns an array if it was able to process
                # a full message, otherwise None will be returned and more data
                # needs to be accumulated
                records = self._process_input(data)
                if records != None:
                    break # we got a full message

            if exceptional and exceptional[0] == self.socket:
                print >>sys.stderr, 'handling exceptional condition for', s.getpeername()
                break

            readable = select.select([self.socket], [], [], 0.5)

        return records


    def _process_input(self, data):
        self.input_buffer.extend(data);
        if len(self.input_buffer) < 8:
            return None

        if data[:3] != 'shc':
            print "Bad magic " + repr(data[:3])

        offset = 3

        pversion = ord(data[offset])
        offset += 1

        if pversion > 1:
            print >>sys.stderr, "Unsupported protocol version ", pversion
            return []

        signed = ''

        header = data[offset]
        offset += 1

        if header == MSG_SIG or header == MSG_CSIG:
            signed = header
            header = data[offset]
            offset += 1

        records = []
        while True:
            chunk_size = struct.unpack('>H', data[offset:offset+2])[0]
            offset += 2

            record = []
            while chunk_size:
                if len(data) < offset + chunk_size + 3:
                    return None
                record.extend(data[offset:offset+chunk_size])
                offset += chunk_size
                chunk_size = struct.unpack('>H', data[offset:offset+2])[0]
                offset += 2

            records.append(record)

            sep = data[offset]
            offset += 1
            if sep == MESSAGE_TERMINATOR:
                break
            if sep != RECORD_SEPARATOR:
                print >>sys.stderr, 'Bad separator ', sep, 'from ', s.getpeername()
                return []


        if signed:
            if len(data) < offset + 8:
                return None
            signature = data[offset:offset + 8]
            offset += 8

        # we have a complete record let's send it back and flush the input accumulator
        self.input_buffer = self.input_buffer[offset:]

        return records


if __name__ == '__main__':
    shard = ShardcacheClient('ln.xant.net', 4443, 'default')
    print shard.get('b.o.txt')
    print shard.sts()

