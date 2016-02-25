package chash_test;

import shardcache.CHash;
import java.util.Arrays;

public class CHashTest {

	public static void main(String[] args) {
		String node_names[] = { "server1", "server2", "server3", "server4", "server5" };

	    int expected[] = {
	    	19236,
			21802,
			21468,
			17602,
			19892,
	    };
	    
	    int servers[] = new int[expected.length];

	    int warns = 0;
	    int fails = 0;


	    CHash chash = new CHash(node_names, 160);

		for (int i = 0; i < 100000; i++) {
			String key = "foo" + i + "\n";
			String node = chash.lookup(key);
			int n_idx = Arrays.asList(node_names).indexOf(node);
			//System.out.printf("%s -- %d \n", node, n_idx);
			servers[n_idx]++;
		}

		for (int i = 0; i < 5; i++) {
			System.out.printf("server%d=%d\n", i + 1, servers[i]);
			if (expected[i] != servers[i]) {
			    System.out.printf("FAIL: expected=%d got=%d\n", expected[i], servers[i]);
			    fails++;
			}
	    }

	    //return (!!warns || !!fails);
	}

}
