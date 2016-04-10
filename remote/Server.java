
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


// todo: if memory is limited, can remove failed or commited txn from the transactionMap
public class Server implements ProjectLib.CommitServing{

    //todo: should it be static given that server might fail?
    public static ProjectLib PL;
    public static AtomicInteger lastTxnID = new AtomicInteger(1);
    public static ConcurrentHashMap<String, Transaction> transactionMap = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Information, AtomicBoolean> globalReplyList = new ConcurrentHashMap<>();

	public void startCommit( String filename, byte[] img, String[] sources ) {
        AtomicInteger reply = new AtomicInteger(0);
		System.out.println( "Server: Got request to commit "+filename );
        int num = sources.length;

        HashMap<String, ArrayList<String>> localSourceMap = new HashMap<>();

        // extract nodes and components
        for (int i = 0; i < num; ++i){
            String str = sources[i];
            int delimeterIdx = str.indexOf(":");

            String node = str.substring(0, delimeterIdx);
            String component = str.substring(delimeterIdx+1);

            if (localSourceMap.containsKey(node)){
                ArrayList<String> components = localSourceMap.get(node);
                components.add(component);
            } else {
                ArrayList<String> components = new ArrayList<>();
                components.add(component);
                localSourceMap.put(node, components);
            }
        }
        final int txnID = lastTxnID.getAndIncrement();
        transactionMap.put(filename, new Transaction(txnID, filename, img, localSourceMap.size()));

        // send inquiry to each component.
        for (Map.Entry<String, ArrayList<String>> entry : localSourceMap.entrySet()){
            String node = entry.getKey();
            ArrayList<String> components = entry.getValue();
            Information inquiry = new Information(txnID, "ASK", filename, node, components, img);
            blockingSendMsg(node, inquiry);
        }

	}

    public static void blockingSendMsg(String dest, Information info){
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Server: send msg to " + dest + " to " + info.action + " id:" + info.txnID + " filename: "+ info.filename + " comps:" + info.componentStr);
//                System.out.println("Sever: send msg to " + dest + " : " + info.toString());
                ProjectLib.Message msg = new ProjectLib.Message(dest, info.getBytes());
                globalReplyList.put(info, new AtomicBoolean(false));
                PL.sendMessage(msg);

                while (true){
                    AtomicBoolean isReply = globalReplyList.get(info);
                    if (isReply.get()){
                        // for the sake of dup msg, don't remove info out of globalReplyList
                        // globalReplyList.remove(info);
                        break;
                    }
                }
            }
        }).start();
    }


    public static void arbitrate(Transaction txn){
//        System.out.println("Time to arbitrate. " + "txnid:" + txn.txnId + " filename:"+ txn.filename);
        boolean consensus = true;
        for (boolean curtReply : txn.answerList.values()){
            consensus &= curtReply;
            if (!consensus){ break; }
        }
        if (consensus) { // if yes, commit
            txn.status = Transaction.TXNStatus.COMMIT;
            System.out.println("Server: txn: " + txn.txnId + " commit. filename: " + txn.filename);
            commit(txn);
            // pre-write file
            try {
                FileOutputStream out = new FileOutputStream(txn.filename);
                out.write(txn.img);
                out.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }else { // if no, release
            System.out.println("Server: txn: " + txn.txnId + " abort. filename: " + txn.filename);
            txn.status = Transaction.TXNStatus.ABORT;
            abort(txn);
        }
    }

    /**
     * tell each node to delete the resources.
     * @param txn
     */
    public static void commit(Transaction txn){
        for (Map.Entry<Transaction.Source, Boolean> entry : txn.answerList.entrySet()){
            String curtNode = entry.getKey().node;
            String curtComponent = entry.getKey().node;
            Information info = new Information(txn.txnId, "COMMIT", txn.filename, curtNode, curtComponent);
            blockingSendMsg(curtComponent, info);

        }
    }

    public static void abort(Transaction txn) {
        for (Map.Entry<Transaction.Source, Boolean> entry : txn.answerList.entrySet()) {
            // only notify those who say yes
            if (entry.getValue().equals(false)){
               continue;
            }
            String curtNode = entry.getKey().node;
            String curtComponent = entry.getKey().node;
            Information info = new Information(txn.txnId, "ABORT", txn.filename, curtNode, curtComponent);
            blockingSendMsg(curtComponent, info);
        }
    }


	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
		
		// main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
//			System.out.println( "Server: Got message from " + msg.addr );
            processMsg(msg);
		}
	}


    /**
     *
     * @param msg
     */
    public static void processMsg(ProjectLib.Message msg){
        String node = msg.addr;
        Information info = new Information(msg.body);
        globalReplyList.get(info).set(true);

        System.out.println("Server: process reply " + info.action + " txnID:" + info.txnID + "from node:" + msg.addr + " filename:" + info.filename + " reply: " + info.reply);
        Transaction txn = transactionMap.get(info.filename);
        if (info.action == Information.actionType.ASK){
            // if it is a ASK msg, then count the txn consensusCnt, and act
            // msg.addr = info.node;
            txn.answerList.put(new Transaction.Source(msg.addr, info.componentStr), info.reply);
            final int replyCnt = txn.consensusCnt.incrementAndGet();
            if (replyCnt == txn.nodeCnt){
                arbitrate(txn);
                // reset and go to next step: COMMIT or ABORT
                txn.consensusCnt.set(0);
                txn.answerList.clear();
            }

        // receive commit ack
        }else if (info.action == Information.actionType.COMMIT || info.action == Information.actionType.ABORT){
            // pre-write has already been conducted
            // txn.Status must be COMMIT
            // msg.addr = info.node;
            final int replyCnt = txn.consensusCnt.incrementAndGet();
            if (replyCnt == txn.nodeCnt){
                txn.consensusCnt.set(0);
                txn.answerList.clear();
                transactionMap.remove(txn);
            }
        }
    }

}

