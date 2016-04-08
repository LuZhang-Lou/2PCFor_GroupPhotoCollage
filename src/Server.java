import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


// todo: if memory is limited, can remove failed or commited txn from the transactionMap
public class Server implements ProjectLib.CommitServing, ProjectLib.MessageHandling {

    //todo: should it be static given that server might fail?
    public static ProjectLib PL;
    public static AtomicInteger lastTXN = new AtomicInteger(0);
    // transactionMap. K:filename  V:Transaction info, including txnid and status
    public static ConcurrentHashMap<String, Transaction> transactionMap = new ConcurrentHashMap<>();

    /*
    filename - name of canddate image file
    img - byte array of candidate image file contents
    sources - string array indicating the contributing files in "source_node:filename" format
     */
	public void startCommit( String filename, byte[] img, String[] sources ) {
        AtomicInteger reply = new AtomicInteger(0);
		System.out.println( "Server: Got request to commit "+filename );
        int num = sources.length;
        String []nodes = new String[num];
        String []components = new String[num];

        // extract nodes and components
        for (int i = 0; i < num; ++i){
            String str = sources[i];
            int delimeterIdx = str.indexOf(":");
            nodes[i] = str.substring(0, delimeterIdx);
            components[i] = str.substring(delimeterIdx);
        }

        final int newTXNID = lastTXN.getAndIncrement();
        transactionMap.put(filename, new Transaction(newTXNID, nodes));

        // send inquiry to each component.
        for (int i = 0; i < num; ++i){
            String node = nodes[i];
            String component = components[i];
            new Thread(new Runnable() {
                @Override
                public void run() {
                    askUserNode(filename, node, component);
                }
            }).start();
        }

        // just return, reply will callback.
	}

    public void askUserNode(String filename, String node, String component){
        StringBuilder sb = new StringBuilder();
        sb.append("SYN:").append(filename).append(":").append(component);
        ProjectLib.Message synMsg = new ProjectLib.Message(node, sb.toString().getBytes());
        PL.sendMessage(synMsg);


        while (true){
            Transaction curtTxn = transactionMap.get(filename);
            curtTxn.
        }
    }


    public boolean deliverMessage(ProjectLib.Message comingMsg){
        String addr = comingMsg.addr;
        String body = new String(comingMsg.body);
        String[] parts = body.split(":");
        if (parts[0].equalsIgnoreCase("SYN:")){
            Transaction txn = transactionMap.get(parts[1]);
            if (transactionMap.get(parts[1]).status == Transaction.TXNStatus.ONGOING){
                txn.consensusCnt.incrementAndGet();
            }

        }
    }

	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			System.out.println( "Server: Got message from " + msg.addr );
			System.out.println( "Server: Echoing message to " + msg.addr );
			PL.sendMessage( msg );

            ProjectLib.Message a = PL.getMessage();

		}
	}
}

