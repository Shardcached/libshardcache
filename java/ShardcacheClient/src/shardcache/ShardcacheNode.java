package shardcache;

public class ShardcacheNode {
	private String label;
	private String address;
	private int port;
	
	public ShardcacheNode(String label, String address, String port) {
		this.label = label;
		this.address = address;
		this.port = Integer.valueOf(port);
	}
	
	public String getLabel() { return this.label; }
	public String getAddress() { return this.address; }
	public int getPort() { return this.port; }
	public String getFullAddress() { return this.address + ":" + this.port; }
	public ShardcacheConnection connect() {
		return new ShardcacheConnection(this);
	}
}
