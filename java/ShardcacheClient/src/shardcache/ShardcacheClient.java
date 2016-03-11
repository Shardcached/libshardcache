package shardcache;

import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.util.Arrays;

import com.sun.istack.internal.Nullable;

import static shardcache.ShardcacheMessage.*;

public class ShardcacheClient {
	private ShardcacheNode[] nodes;
	private CHash cHash;

	public ShardcacheClient(ShardcacheNode[] nodes) {
		this.nodes = nodes;
		String[] nodeNames = new String[nodes.length];
		for (int i = 0; i < nodes.length; i++)
			nodeNames[i] = nodes[i].getLabel();
		this.cHash = new CHash(nodeNames, 200);
	}
	
	public ShardcacheClient(String nodesString) {
		String[] substrings = nodesString.split(",");
		ShardcacheNode[] nodes = new ShardcacheNode[substrings.length];
		String[] nodeNames = new String[substrings.length];
		
		for (int i = 0; i < substrings.length; i++) {
			String str = substrings[i];
			String[] components = str.split(":");
			
			assert(components.length == 3) : 
				"Node string must be of the form <label>:<address>:<port>. " +
				"All fields are mandatory";
			
			String label = components[0];
			String address = components[1];
			String port = components[2];
			
			ShardcacheNode node = new ShardcacheNode(label, address, port);
			nodes[i] = node;
			nodeNames[i] = label;
		}
		this.cHash = new CHash(nodeNames, 100);
		this.nodes = nodes;
	}
	
	ShardcacheNode[] getNodes() {
		return this.nodes;
	}
	
/*
	private String getNodeAddress(String nodeLabel) {
		for (Node node : this.nodes) {
			if (node.getLabel() == nodeLabel)
				return node.getFullAddress();
		}
		return null;
	}
	
	private String getNodeLabel(String nodeAddress) {
		for (Node node : this.nodes) {
			if (node.getFullAddress() == nodeAddress)
				return node.getLabel();
		}
		return null;
	}
*/
	private ShardcacheNode selectNode(String key) {
		String label = this.cHash.lookup(key);
		for (ShardcacheNode node: nodes) {
			if (node.getLabel().equals(label)) {
				return node;
			}
		}
		return null;
	}


	@Nullable
	private ShardcacheMessage sendCommand(Type cmd, String key, byte[]... records) {
        ShardcacheNode owner = this.selectNode(key);
        ShardcacheMessage.Builder builder = new ShardcacheMessage.Builder();
        builder.setMessageType(cmd);
        builder.addRecord(key.getBytes());
		for (byte[] record: records) {
			builder.addRecord(record);
		}

        ShardcacheMessage message = builder.build();

        ShardcacheConnection connection = owner.connect();
        if (connection == null)
            return null;

        connection.send(message);
        ShardcacheMessage response = connection.receive();

		return response;
	}

	@Nullable
	public byte[] get(String key) {
		ShardcacheNode owner = this.selectNode(key);


		ShardcacheMessage response = sendCommand(Type.GET, key);

		if (response == null)
			return null;

        ShardcacheMessage.Record record = response.recordAtIndex(0);
		ShardcacheMessage.Record status = response.recordAtIndex(1);

		return record.getData();
	}

    private int checkResponse(ShardcacheMessage response) {
       if (response != null && response.hdr == HDR_RESPONSE) {
            Record r = response.recordAtIndex(0);
            byte[] rd = r.getData();
            if (rd.length == 1 && rd[0] == 0x00)
                return 0;
        }
        return -1;
    }

    public int set(String key, byte[] data) {
        return checkResponse(sendCommand(Type.SET, key, data));
    }

    public int add(String key, byte[] data) {
        return checkResponse(sendCommand(Type.ADD, key, data));
    }

    public int evict(String key) {
        return checkResponse(sendCommand(Type.EVICT, key));
    }

    public int delete(String key) {
        return checkResponse(sendCommand(Type.DELETE, key));
    }
}
