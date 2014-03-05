##
## SHARDCACHE.PY
##
###############

import select
import socket
import struct
import random
import sys
from chash import CHash
from siphash import SipHash

MSG_GET    = chr(0x01)
MSG_SET    = chr(0x02)
MSG_DEL    = chr(0x03)
MSG_EVI    = chr(0x04)
MSG_OFX    = chr(0x06)
MSG_ADD    = chr(0x07)
MSG_EXI    = chr(0x08)

MSG_CHK    = chr(0x31)
MSG_STS    = chr(0x32)

MSG_SIG    = chr(0xF0)
MSG_CSIG   = chr(0xF1)

MSG_NOOP   = chr(0x90)

RES_OK     = chr(0x00)
RES_YES    = chr(0x01)
RES_EXISTS = chr(0x02)
RES_NO     = chr(0xFE)
RES_ERR    = chr(0xFF)

PROTOCOL_VERSION = chr(0x01)

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

def parse_hosts_string(hosts):
    nodes = []
    node_strings = hosts.split(',')
    for ns in node_strings:
        node_info = ns.split(':')
        if len(node_info) < 3:
            # bad node string
            return None

        nodes.append({ 'label': node_info[0], 'address':node_info[1], 'port':int(node_info[2]) })
    
    return nodes



class ShardcacheClient:
    "Simple Python client for shardcache"

    def __init__(self, hosts, secret=None):
        if secret:
            secret = secret[:16]
            secret += chr(0) * (16 - len(secret))
            self.secret = struct.pack('16c', *secret)
        else:
            self.secret = None
        self.connections = { }
        self.input_buffer = []
        if type(hosts) == str:
            self.nodes = self._parse_hosts_string(hosts)
            if not self.nodes:
                raise Exception('Can\'t parse the hosts string ' + hosts)
        elif type(hosts) == list:
            for node in hosts:
                if type(node) != dict:
                    raise Exception('Node records in the hosts array must be dictionaries!')
                members = ['label', 'address', 'port']
                for m in members:
                    if not node.get(m, None):
                        raise Exception('the \'' + m + '\' member is mandatory in the node structure')
            self.nodes = hosts

        chash_nodes = []
        for node in self.nodes:
            chash_nodes.append(node['label'])
        self.chash = CHash(chash_nodes, 200)
        random.seed()

    def get(self, key):
        records = self._send_message(message = MSG_GET,
                                     records = [key],
                                     node = self.chash.lookup(key))
        if records:
            return ''.join(records[0])

        return None

    def offset(self, key, offset=0, length=0):
        input_records = [key, struct.pack('!L', offset)]
        if length > 0:
            input_records.append(struct.pack('!L', length))

        records = self._send_message(message = MSG_OFX,
                                     records = input_records,
                                     node = self.chash.lookup(key))
        if records:
            return ''.join(records[0])

        return None

  
    def stats(self, node=None):
        if node:
            records = self._send_message(message = MSG_STS, node = node)
            if records:
                return ''.join(records[0])
        else:
            stats = []
            for node in self.nodes:
                node_stats = { }
                records = self._send_message(message = MSG_STS, node = node['label'])
                node_stats = { 'node': node['label'], 'stats': ''.join(records[0]) } if records else { }
                stats.append(node_stats)

            return stats

        return None
 
    def set(self, key, value, expire=None):
        input_records = [key, value]
        if expire:
            input_records.append(struct.pack('!L', expire))
        records = self._send_message(message = MSG_SET,
                                     records = input_records,
                                     node = self.chash.lookup(key))

        if records and records[0] == RES_OK:
            return 0;

        return -1

    def add(self, key, value, expire=None):
        input_records = [key, value]
        if expire:
            input_records.append(struct.pack('!L', expire))
        records = self._send_message(message = MSG_ADD,
                                     records = input_records,
                                     node = self.chash.lookup(key))

        if records:
            return ord(records[0])

        return -1

    def delete(self, key):
        records = self.__send_message(message = MSG_DEL,
                                      records = [key],
                                      node = self.chash.lookup(key))

        if records and records[0] == RES_OK:
            return 0;

        return -1

    def evict(self, key):
        records = self.__send_message(message = MSG_EVI,
                                      records = [key],
                                      node = self.chash.lookup(key))

        if records and records[0] == RES_OK:
            return 0;

        return -1

    def _get_connection(self, host, port):
        host_key = host + ":" + str(port)
        fd = self.connections.get(host_key, None)
        if fd:
            fd.setblocking(1)
            # test if the socket is still valid
            if fd.send(MSG_NOOP) != 1:
                print >>sys.stderr, 'socket ' + str(fd) + ' is not valid anymore'
                fd = None


        if not fd:
            fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                fd.connect((host, int(port)))
                self.connections[host_key] = fd
            except Exception, e:
                print >>sys.stderr, 'Can\'t connect to %s:%d. Exception type is %s' % (host, int(port), `e`)

        return fd

    def _send_message(self, message, records=None, node=None):
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

        packetbuf = 'shc'+PROTOCOL_VERSION # magic + protocol version
        if self.secret: 
            siphash = SipHash(c=2, d=4)
            signature = siphash.auth(struct.unpack('<QQ', self.secret)[0], content)
            packetbuf += MSG_SIG + content + struct.pack('<Q', signature)
        else:
            packetbuf += content

        #print 'packet', repr(packetbuf)

        host = None
        port = 0
        if node:
            for n in self.nodes:
                if n['label'] == node:
                    host = n['address']
                    port = n['port']
                    break
        else:
            index = 0
            if len(self.nodes) > 1:
                index = random.randint(0, len(self.nodes)-1)
            host = self.nodes[index]['address']
            port = self.nodes[index]['port']
            
        conn = self._get_connection(host, port)

        if not conn:
            print >>sys.stderr, 'Can\'t obtain a valid socket to ' + self.host + ':' + str(self.port)
            return None

        conn.setblocking(1)
        conn.sendall(packetbuf)
        conn.setblocking(0)

        # response
        retcords = None
        # read until we have a full message
        readable, writable, exceptions = select.select([conn], [], [], 0.5)
        while readable:
            if readable[0] == conn:
                data = conn.recv(1024)
                # _process_input() will returns an array if it was able to process
                # a full message, otherwise None will be returned and more data
                # needs to be accumulated
                records = self._process_input(data)
                if records != None:
                    break # we got a full message

            if exceptions and exceptions[0] == conn:
                print >>sys.stderr, 'handling exception for', conn.getpeername()
                break

            readable = select.select([conn], [], [], 0.5)

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

        if pversion > PROTOCOL_VERSION:
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
            elif sep != RECORD_SEPARATOR:
                print >>sys.stderr, 'Bad separator ', sep, 'from ', s.getpeername()
                return []


        if signed:
            if len(data) < offset + 8:
                return None
            signature = data[offset:offset + 8]
            offset += 8

        # we have parsed a complete message let's flush the input accumulator
        # and send the records back to the caller
        self.input_buffer = self.input_buffer[offset:]
        return records


if __name__ == '__main__':
    #shard = ShardcacheClient('peer1:ln.xant.net:4443', 'default')
    shard = ShardcacheClient([ { 'label':'peer1', 'address':'ln.xant.net', 'port':4443 },
                               { 'label':'peer3', 'address':'mail.xant.net', 'port':4446 },
                             ], 'default')
    print shard.get('b.o.txt')
    for n in shard.stats():
        print '*** '+ n['node'] + ' ***'
        print n['stats']

    print shard.offset('b.o.txt', 12, 20)

