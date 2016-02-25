package shardcache;

import java.util.Arrays;

public class CHash {
	private String[] nodes;
	private CHashBucket[] blist;
	
	public CHash(String[] nodeNames, int replicas) {
		this.nodes = nodeNames;
		this.blist = new CHashBucket[nodeNames.length * replicas];
		int bidx = 0;
		for (int n = 0; n < nodeNames.length; n++) {
			String nodeName = nodeNames[n];
			for (int r = 0; r < replicas; r++) {
				String rName = r + nodeName;
				long point = LeveldbBloomHash(rName.getBytes());

				blist[bidx++] = new CHashBucket(n, point);
			}
		}
		Arrays.sort(blist);
	}
	
	public String lookup(String key) {
		long point = LeveldbBloomHash(key.getBytes());
		int low = 0;
		int high = this.blist.length;
		// binary search through blist
		while (low < high) {
			int mid = low + (high - low) / 2;
			if (this.blist[mid].getPoint() > point)
				high = mid;
			else
				low = mid + 1;
		}
		
		if (low >= this.blist.length)
			low = 0;
		
		int nodeIdx = this.blist[low].getNodeIdx();
		return this.nodes[nodeIdx];
	}

	// XXX - works only for strings now
	public static long LeveldbBloomHash(byte[] b)
	{
	    long seed = 0xbc9f1d34L;
	    int m = 0xc6a4a793;
	    int len = b.length;
	    long h = (seed ^ len * m) & 0x00000000FFFFFFFF;
	    int offset = 0;
	    while (len >= 4) {
	        h += b[offset + 0] |
	        	 ((b[offset + 1] << 8) & 0x00000000FFFFFFFF)|
	        	 ((b[offset + 2] << 16) & 0x00000000FFFFFFFF)|
	        	 ((b[offset + 3] << 24) & 0x00000000FFFFFFFF);
	        h *= m;
	        h &= 0x00000000FFFFFFFFL;
	        h ^= h >> 16;
	        len -= 4;
	        offset += 4;
	    }
 
	    switch (len) {
	    	case 3:
	    		h += (b[offset + 2] << 16) & 0x00000000FFFFFFFF;
	    	case 2:
	    		h += (b[offset + 1] << 8) & 0x00000000FFFFFFFF;
	    	case 1:
	    		h += b[offset + 0];
	    		h *= m;
		        h &= 0x00000000FFFFFFFFL;
	    		h ^= h >> 24;
	    }
	    return h;
	}
}

class CHashBucket implements Comparable<CHashBucket> {
	private int  nodeIdx;
	private long point;
	public CHashBucket(int nodeIdx, long point) {
		this.nodeIdx = nodeIdx;
		this.point = point;
	}
	
	public int getNodeIdx() { return this.nodeIdx; }
	
	public long getPoint() { return this.point; }
	
	public int compareTo(CHashBucket other) {
		if (this.point < other.point)
			return -1;
		if (this.point > other.point)
			return 1;
		return 0;
	}
}