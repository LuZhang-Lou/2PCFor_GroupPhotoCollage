import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UserNode implements ProjectLib.MessageHandling {
    public static ConcurrentHashMap<String, Integer> imagesStatus;
    public static ConcurrentHashMap<Integer, ArrayList<String>> lockedFiles;
    //    public ConcurrentHashMap<Information, AtomicBoolean> globalReplyList;
    public ReentrantReadWriteLock fileLock;
    public static String myId = "";
//    public static final long DropThreshold = 6100;

    public static ProjectLib PL;
    public static UserNode UN;

    public UserNode(String id)throws  Exception {
        myId = id;
        System.out.println("=================== user: " + myId + " is up:");
        imagesStatus = new ConcurrentHashMap<>();
        lockedFiles = new ConcurrentHashMap<>();
//        globalReplyList = new ConcurrentHashMap<>();
        fileLock = new ReentrantReadWriteLock();

        final File home = new File(".");
        for (final File fileEntry : home.listFiles()) {
            if (!fileEntry.isDirectory() && fileEntry.getName().endsWith(".jpg")) {
                System.out.println("UserNode ID:" + myId + " init file:" + fileEntry.getName());
                imagesStatus.put(fileEntry.getName(), 0);
            }
        }

        rollingback();

    }

    public boolean deliverMessage(ProjectLib.Message msg) {
        Information info = new Information(msg.body);
        processMsg(info);
        return true;
    }

    public static void main(String args[]) throws Exception {
        if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
        UN = new UserNode(args[1]);
        PL = new ProjectLib(Integer.parseInt(args[0]), args[1], UN);
        Runtime.getRuntime().addShutdownHook(new CleanDirHelper());

    }

    public void processMsg(Information info) {
        try {
            Information reply = new Information(info);
            if (info.action == Information.actionType.ASK) {
                // assume one txn one ASK msg.
                if (lockedFiles.containsKey(info.txnID)) {
                    System.out.println("!!!!!!!!!!!!!!call the police!!!!!!!!!!!!!!");
                }
                System.out.println(myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " comps:" + info.componentStr);
                String[] comps = info.componentStr.split("&");
                boolean ret = true;
                ArrayList<String> compList = new ArrayList<>();
                fileLock.writeLock().lock();
                for (String curtComp : comps) {
                    if (!imagesStatus.containsKey(curtComp) || imagesStatus.get(curtComp) != 0) {
                        ret = false;
//                        break;
                    }
                    compList.add(curtComp);
                }
                if (ret) {
                    boolean askRet = PL.askUser(info.img, comps);
                    if (askRet) {
                        lockFile(info.txnID, compList);
                        System.out.println(myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " user accept");
                        reply.reply = true;
                        logHeader(info.txnID, info.filename, compList);
                        logEvent(info.txnID, "ACCEPT");
                    } else {
                        System.out.println(myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " user refuse");
                        reply.reply = false;
                        logHeader(info.txnID, info.filename, compList);
                        logEvent(info.txnID, "REFUSE");
                    }
                } else {
                    System.out.println(myId + ": process ASK. txnid:" + info.txnID + " filename:" + info.filename + " file not exists or is locked");
                    reply.reply = false;
                    logHeader(info.txnID, info.filename, compList);
                    logEvent(info.txnID, "REFUSE");
                }
                fileLock.writeLock().unlock();
                sendMsg(reply, this.myId);
            } else if (info.action == Information.actionType.COMMIT) {
                // might get duplicate commit msg.
                fileLock.writeLock().lock();
                logEvent(info.txnID, "COMMIT");
                if (lockedFiles.containsKey(info.txnID) == false) { // dup msg, resend
                    System.out.println(myId + ": receive dup COMMIT msg. txnid:" + info.txnID + " filename:" + info.filename + "resend..");
                } else { // get this msg first time, process and send reply
                    System.out.println(myId + ": process COMMIT. txnid:" + info.txnID + " filename:" + info.filename);
                    deleteFile(info.txnID);
                }
                fileLock.writeLock().unlock();
                sendMsg(reply, this.myId);
                logEvent(info.txnID, "FINISH");
            } else if (info.action == Information.actionType.ABORT) {
                // might get duplicate commit msg.
                fileLock.writeLock().lock();
                logEvent(info.txnID, "ABORT");
                if (lockedFiles.containsKey(info.txnID) == false) {
                    System.out.println(myId + ": receive dup ABORT msg. txnid:" + info.txnID + " filename:" + info.filename + "resend..");
                } else {
                    System.out.println(myId + ": process ABORT. txnid:" + info.txnID + " filename:" + info.filename);
                    releaseHold(info.txnID);
                }
                fileLock.writeLock().unlock();
                sendMsg(reply, this.myId);
                logEvent(info.txnID, "FINISH");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void lockFile(int txnid, ArrayList<String> comps) {
        lockedFiles.put(txnid, comps);
        for (String curtComp : comps) {
            imagesStatus.put(curtComp, txnid);
        }
    }

    public static void deleteFile(int txnid) {
        ArrayList<String> comps = lockedFiles.get(txnid);
        System.out.println(myId + ": txnid:" + txnid + " in delete. :" + comps.toString());
        for (String curtComp : comps) {
            imagesStatus.remove(curtComp);
            File fileToDelete = new File(curtComp);
            fileToDelete.delete();
            System.out.println(myId + ": txnid:" + txnid + " delete filename:" + curtComp);
            if (fileToDelete.exists()) {
                System.out.println("HELP!!!!!!!!!!!!");
            }
        }
        lockedFiles.remove(txnid);
    }

    public static void releaseHold(int txnid) {
        ArrayList<String> comps = lockedFiles.get(txnid);
        for (String curtComp : comps) {
            imagesStatus.put(curtComp, 0);
        }
        lockedFiles.remove(txnid);
    }

    // usernode side don't have to keep track of whether server receive the msg.
    // b.c. if the msg is dropped, server will resend msg.
    public void sendMsg(Information info, String myID) {
        System.out.println(myID + ": send msg to server to " + info.action + " txnid:" + info.txnID + " filename:" + info.filename + "comps: " + info.componentStr + " reply:" + info.reply);
        ProjectLib.Message msg = new ProjectLib.Message("Server", info.getBytes());
        PL.sendMessage(msg);
    }

    public static void logEvent(int txnID, String event) throws Exception {
        String logName = String.valueOf(txnID) + ".log";
        FileWriter fos = new FileWriter(new File(logName), true);
        BufferedWriter osw = new BufferedWriter(fos);
        osw.write(event);
        osw.newLine();
        osw.flush();
        osw.close();
        PL.fsync();
    }


    public static void logHeader(int txnID, String filename, ArrayList<String> localSourceList) throws IOException {
//        String logName = String.valueOf(txnID)+".log";
        String logName = "txnRecord.log";
        System.out.println("");
        // append
        FileWriter fos = new FileWriter(new File(logName), true);
        BufferedWriter osw = new BufferedWriter(fos);
        StringBuilder sb = new StringBuilder();
        sb.append(txnID).append(" ").append(filename).append(" ");
        for (int i = 0; i < localSourceList.size() - 1; ++i) {
            sb.append(localSourceList.get(i)).append(":");
            System.out.println(myId + ": in logHeader, txn:" + txnID + " log component :" + localSourceList.get(i));
        }
        sb.append(localSourceList.get(localSourceList.size() - 1));
        System.out.println(myId + ": in logHeader, txn:" + txnID + " log component :" + localSourceList.get(localSourceList.size()-1));
        osw.write(sb.toString());
        osw.newLine();
        osw.flush();
        osw.close();
        PL.fsync();
    }


    public static void rollingback() throws Exception {
        // read "txnRecord.log"
        File recordFile = new File("txnRecord.log");
        if (recordFile.exists() == false) {
            System.out.println(myId + ": No historic record.");
            return;
        }
        FileReader fis = new FileReader(recordFile);
        BufferedReader isw = new BufferedReader(fis);
        String line;
        while ((line = isw.readLine()) != null) {
            String[] parts = line.split(" ");
            if (parts.length != 3) {
                throw new Exception("Log corrupted.");
            }
            int txnID = Integer.parseInt(parts[0]);
            // check this txn's status first.

            File curTxnLog = new File(txnID + ".log");
            if (curTxnLog.exists() == false){
                System.out.println("rollingback -- txn:" + txnID + "missing" );
                continue;
            }
            FileReader readerForEachTxn = new FileReader(curTxnLog);
            BufferedReader brForEachTxn = new BufferedReader(readerForEachTxn);
            String innerLine;
            String status = "";
            while ((innerLine = brForEachTxn.readLine()) != null) {
                if (innerLine.equalsIgnoreCase("COMMIT")) {
                    status = "COMMIT";
                } else if (innerLine.equalsIgnoreCase("ABORT")) {
                    status = "ABORT";
                } else if (innerLine.equalsIgnoreCase("ACCEPT")) {
                    status = "ACCEPT";
                } else if (innerLine.equalsIgnoreCase("REFUSE")) {
                    status = "REFUSE";
                }
            }
            brForEachTxn.close();
            readerForEachTxn.close();
            curTxnLog.delete();
            if (curTxnLog.exists()) {
                System.out.println("HELP!!!!!!!!!!!!");
            }

            System.out.println("rollingback -- txn:" + txnID + " " + status);
            if (status.equalsIgnoreCase("REFUSE") || status.equalsIgnoreCase("ABORT")) {
                System.out.println("rollingback -- txn:" + txnID + " " + status);
                continue;
            }

            String filename = parts[1];
            String sourceInOne = parts[2];
            String[] components = sourceInOne.split(";");
            ArrayList<String> localSourceList = new ArrayList<>();
            String node = components[0];
            for (int i = 1; i < components.length - 1; ++i) {
                localSourceList.add(components[i]);
                System.out.println(myId + ": in rollingback, txn:"+ txnID + " find component :" + localSourceList.get(localSourceList.size()-1));
            }
            localSourceList.add(components[components.length-1]);
            System.out.println(myId + ": in rollingback, txn:"+ txnID + " find component :" + localSourceList.get(localSourceList.size()-1));

            if (status.equalsIgnoreCase("ACCEPT")) {
                System.out.println(myId + ": rollingback find "+ txnID +" accept, lock:" + localSourceList.toString());
                lockFile(txnID, localSourceList);
            } else if (status.equalsIgnoreCase("COMMIT")) {
                System.out.println(myId + ": rollingback find "+ txnID +" commit, delete files: " + localSourceList.toString());
                lockFile(txnID, localSourceList);
                deleteFile(txnID);
            }


        } // end of a line

        fis.close();
        isw.close();
        recordFile.delete();
        if (recordFile.exists()) {
            System.out.println("HELP!!!!!!!!!!!!!!!");
        }
    }

    static class CleanDirHelper extends Thread{

        public void run() {
            // clean up dirs
            final File home = new File(".");
            for (final File fileEntry : home.listFiles()) {
                if (fileEntry.isDirectory()){
                    System.out.println(myId + " rm dir:" + fileEntry.getName());
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

