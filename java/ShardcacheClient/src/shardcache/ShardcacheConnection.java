package shardcache;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import com.sun.istack.internal.Nullable;
import com.sun.tools.javac.util.ArrayUtils;

public class ShardcacheConnection {
	private DataOutputStream outToServer;
	private DataInputStream inFromServer;
	public ShardcacheConnection(ShardcacheNode node) {
		try {
			Socket clientSocket = new Socket(node.getAddress(), node.getPort());
			outToServer = new DataOutputStream(clientSocket.getOutputStream());
			inFromServer = new DataInputStream(clientSocket.getInputStream());
		} catch(IOException e) {
		
		}
	}

    @Nullable
	public ShardcacheMessage receive() {
		byte[] buf = new byte[256];
		try {
            ShardcacheMessage.Builder builder = new ShardcacheMessage.Builder();
			int ret = inFromServer.read(buf);
			while (ret > 0) {
                ShardcacheMessage.Builder.Status status = builder.parse(buf, ret);
                if (status != ShardcacheMessage.Builder.Status.NEEDS_DATA)
                    break;
				ret = inFromServer.read(buf);
			}
            return builder.build();
		} catch (Exception e) {
			// TODO  - report the error
		}
		return null;
	}
	
	public int send(ShardcacheMessage msg) {
		try {
			outToServer.write(msg.data);
			return 0;
		} catch (IOException e) {
			return -1;
		}
	}	
	
}