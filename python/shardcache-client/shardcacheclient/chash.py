from pprint import pprint

def leveldb_bloom_hash(key):
    seed = 0xbc9f1d34
    m = 0xc6a4a793

    b = [ ord(byte) for byte in bytes(key) ]
    size = len(key)
    h = (seed ^ size * m) & 0xFFFFFFFF

    while size >= 4:
        h = (h + (b[0] | (b[1] << 8) | (b[2] << 16) | (b[3] << 24))) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h = (h ^ (h >> 16)) & 0xFFFFFFFF
        b = b[4:]
        size -= 4
 
    if size == 3:
        h = (h + (b[2] << 16)) & 0xFFFFFFFF
    if size >= 2:
        h = (h + (b[1] << 8)) & 0xFFFFFFFF
    if size >= 1:
        h = (h + b[0]) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h = (h ^ (h >> 24)) & 0xFFFFFFFF

    return h

class CHash:
    "Consistent Hashing module"

    def __init__(self, nodes, replicas):
        if type(nodes) != list:
            raise Exception('\'nodes\' MUST be a list!')
        buckets = []
        for node in nodes:
            for i in range(replicas):
                key = str(i) + node
                point = leveldb_bloom_hash(key)
                buckets.append({ 'name':node, 'point':point })
        self.buckets = sorted(buckets, key = lambda d: d['point'])

    def lookup(self, key):
        point = leveldb_bloom_hash(key)
        low = 0
        high = len(self.buckets)

        while low < high:
            mid = low + (high - low) / 2
            if self.buckets[mid]['point'] > point:
                high = mid
            else:
                low = mid + 1

        if low >= len(self.buckets):
            low = 0

        return self.buckets[low]['name']


if __name__ == '__main__':
    c = CHash(['server1', 'server2', 'server3', 'server4', 'server5'], 160)

    expected = [ 19236, 21802, 21468, 17602, 19892 ]
    totals = [ 0, 0, 0, 0, 0 ]

    #pprint(c.buckets)

    for i in range(100000):
        string = "foo" + str(i) + "\n"
        node = c.lookup(string)
        b = bytes(node)
        n_idx = int(b[6]) - 1
        totals[n_idx] += 1

    for i in range(5):
        print "server" + str(i+1) + "=" + str(totals[i])
        if expected[i] != totals[i]:
            print "Fail: expected:" + str(expected[i]) + " got=" + str(totals[i])

