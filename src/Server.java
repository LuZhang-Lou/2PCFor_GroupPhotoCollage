import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


// todo: if memory is limited, can remove failed or commited txn from the transactionMap
public class Server implements ProjectLib.CommitServing{

    //todo: should it be static given that server might fail?
    public static ProjectLib PL;
//    public static AtomicInteger lastTXN = new AtomicInteger(0);
    public static ConcurrentHashMap<String, Transaction> transactionMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Information, Boolean> globalReplyList = new ConcurrentHashMap<>();

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

        transactionMap.put(filename, new Transaction(filename, nodes, components));

        // send inquiry to each component.
        for (int i = 0; i < num; ++i){
            String node = nodes[i];
            String component = components[i];
            Information inquiry = new Information("ASK", filename, node, component);
            blockingSendMsg(node, inquiry);

        }
	}

    public void blockingSendMsg(String dest, Information info){
        new Thread(new Runnable() {
            @Override
            public void run() {
                ProjectLib.Message msg = new ProjectLib.Message(dest, info.getBytes());

                globalReplyList.put(info, false);
                PL.sendMessage(msg);

                while (true){
                    boolean isReply = globalReplyList.get(info);
                    if (isReply){
                        // if it is a ASK msg, then count the txn consensusCnt, and act
                        if (info.action == Information.actionType.ASK){
                            Transaction curtTxn = transactionMap.get(info.filename);
                            final int replyCnt = curtTxn.consensusCnt.incrementAndGet();
                            if (replyCnt == curtTxn.nodeCnt){
                                arbitrate(curtTxn);
                            }
                        }
                        // remove this information from globalMsgQueue
                        globalReplyList.remove(info);
                        break;
                    }
                }
            }
        }).start();
    }


    public void arbitrate(Transaction txn){
        boolean consensus = true;
        for (boolean curtReply : txn.answerList.values()){
            consensus &= curtReply;
            if (!consensus){ break; }
        }
        if (consensus) { // if yes
            // commit


        }else {          // if no
            // release
        }
    }

    /**
     * tell each node to delete the resources.
     * @param txn
     */
    public void commit(Transaction txn){
        for (Map.Entry<Transaction.Source, Boolean> entry : txn.answerList.entrySet()){
            String curtNode = entry.getKey().node;
            String curtComponent = entry.getKey().node;
            Information info = new Information("COMMIT", txn.filename, curtNode, curtComponent);
            blockingSendMsg(curtComponent, info);

        }
    }

    public void abort(Transaction txn) {
        for (Map.Entry<Transaction.Source, Boolean> entry : txn.answerList.entrySet()) {
            // only notify those who say yes
            if (entry.getValue().equals(false)){
               continue;
            }
            String curtNode = entry.getKey().node;
            String curtComponent = entry.getKey().node;
            Information info = new Information("ABORT", txn.filename, curtNode, curtComponent);
            blockingSendMsg(curtComponent, info);
        }
    }

   /*
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
    }*/

	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
			System.out.println( "Server: Got message from " + msg.addr );


            ProjectLib.Message a = PL.getMessage();

		}
	}

    public
}

