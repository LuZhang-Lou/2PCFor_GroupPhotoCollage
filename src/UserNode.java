import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UserNode implements ProjectLib.MessageHandling {
    public ConcurrentHashMap<String, Integer> imagesStatus;
    public ConcurrentHashMap<Integer, ArrayList<String>> lockedFiles;
    public ConcurrentHashMap<Information, AtomicBoolean> globalReplyList;
    public ReentrantReadWriteLock fileLock;
	public final String myId;

    public static ProjectLib PL;
    public static UserNode UN;

	public UserNode( String id ) {
		myId = id;
        imagesStatus = new ConcurrentHashMap<>();
        lockedFiles = new ConcurrentHashMap<>();
        globalReplyList = new ConcurrentHashMap<>();
        fileLock = new ReentrantReadWriteLock();

        final File home = new File(".");
        for (final File fileEntry : home.listFiles()) {
            if (!fileEntry.isDirectory() && fileEntry.getName().endsWith(".jpg")) {
                System.out.println("UserNode ID:" + myId + " init file:" + fileEntry.getName());
                imagesStatus.put(fileEntry.getName(), 0);
            }
        }

	}

	public boolean deliverMessage( ProjectLib.Message msg ) {
        Information info = new Information(msg.body);
        processMsg(info);
		return true;
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
	}

    public void processMsg(Information info){
        try {
            Information reply = new Information(info);
            if (info.action == Information.actionType.ASK) {
                System.out.println( myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " comps:" + info.componentStr);
                String[] comps = info.componentStr.split("&");
                boolean ret = true;
                ArrayList<String> compList = new ArrayList<>();
                fileLock.writeLock().lock();
                for (String curtComp : comps) {
//                    System.out.println( myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " curtComps:" + curtComp);
                    if (!imagesStatus.containsKey(curtComp) || imagesStatus.get(curtComp) != 0) {
                        ret = false;
                        break;
                    }
                    compList.add(curtComp);
                }

//                if (ret && PL.askUser(info.img, comps)) {
                if (ret){
                    boolean askRet = PL.askUser(info.img, comps);
                    if (askRet) {
                        lockFile(info.txnID, compList);
                        System.out.println( myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " user accept");
                        reply.reply = true;
                    }else{
                        System.out.println( myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " user refuse");
                        reply.reply = false;
                    }
                } else {
                    System.out.println( myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " file not exists or is locked");
                    reply.reply = false;
                }
                fileLock.writeLock().unlock();
                blockingSendMsg(reply, this.myId);
            } else if (info.action == Information.actionType.COMMIT) {
                System.out.println( myId + ": process COMMIT. txnid:" + info.txnID + " filename:" + info.filename);
                fileLock.writeLock().lock();
                deleteFile(info.txnID);
                fileLock.writeLock().unlock();
                blockingSendMsg(reply, this.myId);
            } else if (info.action == Information.actionType.ABORT) {
                System.out.println( myId + ": process ABORT. txnid:" + info.txnID + " filename:" + info.filename);
                fileLock.writeLock().lock();
                releaseHold(info.txnID);
                fileLock.writeLock().unlock();
                blockingSendMsg(reply, this.myId);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void lockFile(int txnid, ArrayList<String> comps){
        lockedFiles.put(txnid, comps);
        for (String curtComp : comps) {
            imagesStatus.put(curtComp, txnid);
        }
    }

    public void deleteFile(int txnid){
        System.out.println( myId + ": txnid:" + txnid + " in delete.");
        ArrayList<String> comps = lockedFiles.get(txnid);
        for (String curtComp : comps){
            imagesStatus.remove(curtComp);
            File fileToDelete = new File(curtComp);
            fileToDelete.delete();
            System.out.println( myId + ": txnid:" + txnid + " delete filename:" +curtComp);
            if (fileToDelete.exists()){
                System.out.println("HELP!!!!!!!!!!!!");
            }
        }
        lockedFiles.remove(txnid);
    }

    public void releaseHold(int txnid){
        ArrayList<String> comps = lockedFiles.get(txnid);
        for (String curtComp : comps){
            imagesStatus.put(curtComp, 0);
        }
        lockedFiles.remove(txnid);
    }


    // todo : add flag to indicate whether to wait for ack?
    public void blockingSendMsg(Information info, String myID){
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println(myID + " : send msg to server to " + info.action + " txnid:" + info.txnID + " filename:" + info.filename + "comps: " + info.componentStr + " reply:"+ info.reply) ;
//                System.out.println(myID + " : send msg to server : " + info.toString());
                ProjectLib.Message msg = new ProjectLib.Message("Server", info.getBytes());
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

}

