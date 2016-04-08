/* Skeleton code for Server */

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

public class Server implements ProjectLib.CommitServing {


    public enum TXNStatus {ONGOING, FAILED, COMMIT};

    //todo: should it be static given that server might fail?
    public static ProjectLib PL;
    public static AtomicInteger lastTXN = new AtomicInteger(0);
    public static ConcurrentHashMap<Integer, TXNStatus> transactionMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Integer, Integer> commitMap = new ConcurrentHashMap<>();

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
        transactionMap.put(newTXNID, TXNStatus.ONGOING);

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

