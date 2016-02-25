package shardcachec;

import shardcache.*;

public class shardcachec {

	public static void usage() {
		System.out.print("Usage: shardcachec <Command> <Key> \n" +
				         "   Commands: \n" +
		                 "        get       <key> [ <output_file> (defaults to stdout) ] \n" +
		                 "        getf      <key> [ <output_file> (defaults to stdout) ] \n" +
		                 "        get_async <key> [ <output_file> (defaults to stdout) ] \n" +
		                 "        get_multi <key> [ <key> ... ] [ -o <output_dir>   (defaults to stdout) ] \n" +
		                 "        offset    <key> <offset> <kength> [ <output_file> (defaults to stdout) \n" +
		                 "        set       <key> [ -e <expire> ] [ -i <input_file> (defaults to stdin) ] \n" +
		                 "        add       <key> [ -e <expire> ] [ -i <input_file> (defaults to stdin) ] \n" +
		                 "        exists    <key> \n" +
		                 "        touch     <key> \n" +
		                 "        del       <key> \n" +
		                 "        evict     <key> \n" +
		                 "        index   [ <node> ] \n" +
		                 "        stats   [ <node> ] \n" +
		                 "        check   [ <node> ] \n");
		System.exit(-1);
	}
	
	public static void main(String[] args) {
		
		String shcHosts = System.getenv("SHC_HOSTS");
		String[] nodes = shcHosts.split(",");
		String[] labels = new String[nodes.length];
		int cnt = 0;
		for (String node: nodes) {
			String[] comps = node.split(":");
			labels[cnt++] = comps[0];
		}		
		
		if (args.length < 1)
			usage();
		// TODO Auto-generated method stub
		ShardcacheClient client = new ShardcacheClient(shcHosts);
		
		String command = args[0];
		if (command.equals("get")) {
			if (args.length < 2) {
				
			}
			byte[] response = client.get(args[1]);
			String out = null;
			try {
				out = new String(response, "UTF-8");
			} catch (Exception e) {
				System.out.println("Can't convert the response to a valid string");
				System.exit(-1);
			}
			System.out.printf("%s", out);
		} else if (command.equalsIgnoreCase("getf")) {
		
		} else if (command.equalsIgnoreCase("get_async")) {
			
		} else if (command.equalsIgnoreCase("get_multi")) {
			
		} else if (command.equals("offset")) {
			
		} else if (command.equals("set")) {

			if (args.length < 4) {
			
			}
			int status = client.set(args[1], args[2].getBytes());

		} else if (command.equals("add")) {
			
		} else if (command.equals("stats")) {
		
			
		} else if (command.equals("exists")) {
			
		} else if (command.equals("touch")) {
			
		} else if (command.equals("del")) {
			
		} else if (command.equals("evict")) {
			
		} else if (command.equals("index")) {
			
		} else if (command.equals("stats")) {
			
		} else if (command.equals("check")) {
			
		} else {
			System.out.printf("Unknown command %s.\n", command);
			usage();
		}
		
	}

}
