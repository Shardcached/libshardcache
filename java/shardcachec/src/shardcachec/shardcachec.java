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

	private static void checkArgsNumber(String[] args, int expectedNum) {
        if (args.length < 4) {
            System.out.print("Not enough arguments\n");
        }
        usage();
	}

    private static void checkResponse(ShardcacheMessage.Type cmd, int status) {

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
            checkArgsNumber(args, 2);
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
			checkArgsNumber(args, 4);
            checkResponse(ShardcacheMessage.Type.SET, client.set(args[1], args[2].getBytes()));
		} else if (command.equals("add")) {
            checkResponse(ShardcacheMessage.Type.ADD, client.add(args[1], args[2].getBytes()));
		} else if (command.equals("stats")) {
		
			
		} else if (command.equals("exists")) {
			
		} else if (command.equals("touch")) {
			
		} else if (command.equals("del")) {
			checkResponse(ShardcacheMessage.Type.DELETE, client.delete(args[1]));
		} else if (command.equals("evict")) {
            checkResponse(ShardcacheMessage.Type.EVICT, client.evict(args[1]));
		} else if (command.equals("index")) {
            checkResponse(ShardcacheMessage.Type.EVICT, client.evict(args[1]));
		} else if (command.equals("stats")) {
            //checkResponse(ShardcacheMessage.Type.STATS, client.evict(args[1]));
		} else if (command.equals("check")) {
            //checkResponse(ShardcacheMessage.Type.CHECK, client.evict(args[1]));
		} else {
			System.out.printf("Unknown command %s.\n", command);
			usage();
		}
		
	}

}
