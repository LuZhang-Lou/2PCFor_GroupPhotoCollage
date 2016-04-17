

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class Server implements ProjectLib.CommitServing{

    public static ProjectLib PL;
    public static AtomicInteger lastTxnID = new AtomicInteger(1);
    public static ConcurrentHashMap<String, Transaction> transactionMap =
                                                new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Information, AtomicBoolean> globalReplyList =
                                                new ConcurrentHashMap<>();
    public static final long DropThreshold = 3000;

    /**
     * start a transaction
     * @param filename filename
     * @param img      img content
     * @param sources  img source list
     */
	public void startCommit( String filename, byte[] img, String[] sources ) {
        AtomicInteger reply = new AtomicInteger(0);
		System.out.println( "Server: Got request to commit "+filename );
        int num = sources.length;

        // prewrite image. assume unique filename
        try {
            FileOutputStream out = new FileOutputStream(filename);
            out.write(img);
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }

        HashMap<String, ArrayList<String>> localSourceMap = new HashMap<>();
        ArrayList<String> localSourceList = new ArrayList<>();
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
                localSourceList.add(node);
            }
        }
        final int txnID = lastTxnID.getAndIncrement();

        //log
        try {
            logHeader(txnID, filename, localSourceMap);
            logEvent(txnID, "START");
        }catch (Exception e){
            e.printStackTrace();
        }
        transactionMap.put(filename, new Transaction(txnID, filename, img,
                                    localSourceMap.size(), localSourceList));

        // send inquiry to each component.
        for (Map.Entry<String, ArrayList<String>> entry : localSourceMap.entrySet()){
            String node = entry.getKey();
            ArrayList<String> components = entry.getValue();
            Information inquiry = new Information
                                (txnID, "ASK", filename, node, components, img);
            blockingSendMsg(node, inquiry);
        }

	}

    /**
     * send blocking message and wait for receive
     * @param dest destination
     * @param info information body
     */
    public static void blockingSendMsg(String dest, Information info){
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("Server: send msg to " + dest + " to "
                                    + info.action + " id:" + info.txnID
                                    + " filename: "+ info.filename + " comps:"
                                    + info.componentStr);
                ProjectLib.Message msg = new ProjectLib.Message(dest, info.getBytes());
                globalReplyList.put(info, new AtomicBoolean(false));
                PL.sendMessage(msg);

                try {
                    long lastSendTime = System.currentTimeMillis();
                    while (true) {
                        AtomicBoolean isReply = globalReplyList.get(info);
                        if (isReply.get()) {
                            globalReplyList.remove(info);
                            break;
                        }
                        // if timeout
                        if (System.currentTimeMillis() - lastSendTime > DropThreshold) {
                            // if in phase1, timeout => no, then abort
                            if (info.action == Information.actionType.ASK) {
                                Transaction txn = transactionMap.get(info.filename);
                                System.out.println("Server: txn: " + txn.txnId
                                                    + " in phase 1 dest:" + info.node
                                                    + " time out. => abort filename: "
                                                    + txn.filename);
                                txn.status = Transaction.TXNStatus.ABORT;
                                abort(txn);
                                txn.consensusCnt.set(0);
                                txn.answerList.clear();
                                globalReplyList.remove(info);
                                break;
                            }else { // in phase2, commit or abort. resend.
                                System.out.println("Server: txn: " + info.txnID
                                                    + " in phase 2 dest:" + info.node
                                                    + " time out. => resend  filename: "
                                                    + info.filename);
                                PL.sendMessage(msg);
                                lastSendTime = System.currentTimeMillis();
                            }
                        }
                        Thread.sleep(50);
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        }).start();
    }


    /**
     * decide whether a txn commits or aborts
     * @param txn transaction
     */
    public static void arbitrate(Transaction txn){
        boolean consensus = true;
        for (boolean curtReply : txn.answerList.values()){
            consensus &= curtReply;
            if (!consensus){ break; }
        }
        if (consensus) { // if yes, commit
            txn.status = Transaction.TXNStatus.COMMIT;
            System.out.println("Server: txn: " + txn.txnId + " commit. filename: "
                                + txn.filename);
            commit(txn);
        }else { // if no, release
            System.out.println("Server: txn: " + txn.txnId + " abort. filename: "
                                + txn.filename);
            txn.status = Transaction.TXNStatus.ABORT;
            abort(txn);
        }
    }

    /**
     * notify related userNodes to commit transaction
     * @param txn transaction
     */
    public static void commit(Transaction txn){
        try {
                logEvent(txn.txnId, "COMMIT");
            }catch (Exception e){
                e.printStackTrace();
            }

        for (Map.Entry<String, Boolean> entry : txn.ifAnswerList.entrySet()){
            String curtNode = entry.getKey();
            String curtComponent = "";
            Information info = new Information(txn.txnId, "COMMIT", txn.filename,
                                                curtNode, curtComponent);
            blockingSendMsg(curtNode, info);
        }
    }

    /**
     * notify related userNOdes to abort transaction
     * @param txn
     */
    public static void abort(Transaction txn) {
        System.out.println("Server: abort txn " + txn.txnId);

        // delete pre-write files.
        File fileToDelete = new File(txn.filename);
        fileToDelete.delete();
        System.out.println( "Server: txnid:" + txn.txnId + " delete pre-writefilename:"
                            + txn.filename);

        try{
           logEvent(txn.txnId, "ABORT");
        }catch (Exception e){
            e.printStackTrace();
        }

        // notify those say yes
        for (Map.Entry<Transaction.Source, Boolean> entry : txn.answerList.entrySet()) {
            if (entry.getValue().equals(false)){
               continue;
            }
            String curtNode = entry.getKey().node;
            Information info = new Information(txn.txnId, "ABORT", txn.filename
                                , curtNode, "");
            blockingSendMsg(curtNode, info);
        }
        // notify those haven't reply or dropped
        for (Map.Entry<String, Boolean> entry : txn.ifAnswerList.entrySet()) {
            if (entry.getValue().equals(true)){
               continue;
            }
            String curtNode = entry.getKey();
            Information info = new Information(txn.txnId, "ABORT", txn.filename
                                , curtNode, "");
            blockingSendMsg(curtNode, info);
        }
    }


    /**
     * main function for UserNode
     * @param args
     * @throws Exception
     */
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		Server srv = new Server();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv );
        System.out.println("===================Server is up:");

        rollingback();
        Runtime.getRuntime().addShutdownHook(new UserNode.CleanDirHelper("Server"));

        // main loop
		while (true) {
			ProjectLib.Message msg = PL.getMessage();
            processMsg(msg);
		}
	}


    /**
     * process msg
     * @param msg message
     */
    public static void processMsg(ProjectLib.Message msg){
        String node = msg.addr;
        Information info = new Information(msg.body);
        System.out.println("Server: process reply " + info.action + " txnID:"
                + info.txnID + " from node:" + msg.addr + " filename:"
                + info.filename + " reply: " + info.reply);
        if (globalReplyList.containsKey(info)) {
            globalReplyList.get(info).set(true);
        } // else, dropped ASK msg, ignore...
        else {
            System.out.println("Server: received dropped/delayed ask msg, ignore");
            return;
        }

        Transaction txn = transactionMap.get(info.filename);
        if (info.action == Information.actionType.ASK){
            // if it is a ASK msg, then count the txn consensusCnt, and act
            txn.answerList.put(new Transaction.Source(msg.addr, info.componentStr),
                                info.reply);
            txn.ifAnswerList.put(msg.addr, true);
            final int replyCnt = txn.consensusCnt.incrementAndGet();
            if (replyCnt == txn.nodeCnt){
                arbitrate(txn);
                // reset and go to next step: COMMIT or ABORT
                txn.consensusCnt.set(0);
                txn.answerList.clear();
            }

        // receive commit ack
        }else if (info.action == Information.actionType.COMMIT ||
                    info.action == Information.actionType.ABORT){
            // pre-write has already been conducted
            final int replyCnt = txn.consensusCnt.incrementAndGet();
            if (replyCnt == txn.nodeCnt){
                transactionMap.remove(txn);
                try{
                    logEvent(txn.txnId, "FINISH");
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * log transaction's information into "txnRecord.log"
     * @param txnID transactionID
     * @param filename filename
     * @param localSourceMap source images map
     * @throws IOException
     */
    public static void logHeader(int txnID, String filename, HashMap<String,
                                ArrayList<String>> localSourceMap)
                                throws IOException {
        String logName = "txnRecord.log";
        // append
        FileWriter fos = new FileWriter(new File(logName), true);
        BufferedWriter osw = new BufferedWriter(fos);
        StringBuilder sb = new StringBuilder();
        sb.append(txnID).append(" ").append(filename).append(" ");
        for (Map.Entry<String, ArrayList<String>> entry:
                localSourceMap.entrySet()){
            sb.append(entry.getKey()).append(":");
            ArrayList<String> comps = entry.getValue();
            for (int i = 0; i < comps.size() -1; ++i){
                sb.append(comps.get(i)).append(":");
            }
            sb.append(comps.get(comps.size()-1));
            sb.append(";");
        }
        osw.write(sb.toString());
        osw.newLine();
        osw.flush();
        osw.close();
        PL.fsync();
    }

    /**
     * log event into this transaction's log file
     * @param txnID transactionID
     * @param event event
     * @throws Exception
     */
    public static void logEvent(int txnID, String event) throws Exception{
        String logName = String.valueOf(txnID)+".log";
        FileWriter fos = new FileWriter(new File(logName), true);
        BufferedWriter osw = new BufferedWriter(fos);
        osw.write(event);
        osw.newLine();
        osw.flush();
        osw.close();
        PL.fsync();
    }

    /**
     * rollingback to last checkpoint
     * @throws Exception
     */
    public static void rollingback() throws Exception{
        // read "txnRecord.log"
        File recordFile = new File("txnRecord.log");
        if (recordFile.exists() == false){
            System.out.println("Server: No historic record.");
            return;
        }
        FileReader fis = new FileReader(recordFile);
        BufferedReader isw = new BufferedReader(fis);
        String line;
        while((line = isw.readLine()) != null) {
            String[] parts = line.split(" ");
            if (parts.length != 3){
                throw new Exception("Log corrupted.");
            }
            int txnID = Integer.parseInt(parts[0]);
            lastTxnID.set(txnID);

            // check this txn's status first.
            File curTxnLog = new File(txnID+".log");
            if (curTxnLog.exists() == false){
                // stale data
                continue;
            }
            FileReader readerForEachTxn = new FileReader(curTxnLog);
            BufferedReader brForEachTxn = new BufferedReader(readerForEachTxn);
            String innerLine;
            Transaction.TXNStatus status = Transaction.TXNStatus.INQUIRY;
            while ((innerLine = brForEachTxn.readLine()) != null){
                if (innerLine.equalsIgnoreCase("COMMIT")){
                    status = Transaction.TXNStatus.COMMIT;
                }else if (innerLine.equalsIgnoreCase("ABORT")){
                    status = Transaction.TXNStatus.ABORT;
                }else if (innerLine.equalsIgnoreCase("FINISH")){
                    status = Transaction.TXNStatus.FINISH;
                }
            }
            readerForEachTxn.close();
            brForEachTxn.close();
            curTxnLog.delete();

            System.out.println("rollingback -- txn:" + txnID + " " + status);
            if (status == Transaction.TXNStatus.FINISH){
                continue;
            }

            String filename = parts[1];
            byte[] useless = new byte[1];
            String sourceInOne = parts[2];
            String [] components = sourceInOne.split(";");
            ArrayList<String> localSourceList = new ArrayList<>();
            for (String singleNode : components){
                String [] innerParts = singleNode.split(":");
                localSourceList.add(innerParts[0]);
                System.out.println("Server: rolling back, find:" + singleNode +
                                    " involved in txn:" + txnID);
            }
            Transaction txn =  new Transaction(txnID, filename, useless,
                                    components.length, localSourceList, status);
            transactionMap.put(filename, txn);
            if (status == Transaction.TXNStatus.ABORT ||
                status == Transaction.TXNStatus.INQUIRY){
                abort(txn);
            } else if (status == Transaction.TXNStatus.COMMIT){
                commit(txn);
            }


        } // end of a line
        fis.close();
        isw.close();

        lastTxnID.incrementAndGet();
        System.out.println("Server: rolling back lasttxnid:" + lastTxnID);
    }

    /**
     * clean working direcotry helper class
     */
    static class CleanDirHelper extends Thread{

        public void run() {
            // clean up dirs
            final File home = new File(".");
            for (final File fileEntry : home.listFiles()) {
                if (fileEntry.isDirectory()){
                    System.out.println("Service: rm dir:" + fileEntry.getName());
                    rmDir(fileEntry);
                }
            }
        }
        private static boolean rmDir(File dir) {
            if (dir.isDirectory()) {
                String[] children = dir.list();
                for (int i = 0; i < children.length; i++) {
                    boolean ret = rmDir(new File(dir, children[i]));
                    if (ret == false) {
                        return false;
                    }
                }
            }
            return dir.delete();
        }
    }

}

