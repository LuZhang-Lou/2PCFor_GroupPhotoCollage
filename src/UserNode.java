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
    public ReentrantReadWriteLock fileLock;
    public static String myId = "";
    public static ProjectLib PL;
    public static UserNode UN;


    public UserNode(String id)throws  Exception {
        myId = id;
        System.out.println("=================== user: " + myId + " is up:");
        imagesStatus = new ConcurrentHashMap<>();
        lockedFiles = new ConcurrentHashMap<>();
        fileLock = new ReentrantReadWriteLock();


        // pre-read existing files into memory.
        final File home = new File(".");
        for (final File fileEntry : home.listFiles()) {
            if (!fileEntry.isDirectory() &&
                    fileEntry.getName().endsWith(".jpg")) {
                System.out.println("UserNode ID:" + myId
                                    + " init file:" + fileEntry.getName());
                imagesStatus.put(fileEntry.getName(), 0);
            }
        }
        // rolling back to last checkpoints.
        rollingback();
    }

    /**
     * message Handler
     * @param msg
     * @return true, don't put this msg again in the queue
     */
    public boolean deliverMessage(ProjectLib.Message msg) {
        Information info = new Information(msg.body);
        processMsg(info);
        return true;
    }

    /**
     * Main function of UserNode
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
        UN = new UserNode(args[1]);
        PL = new ProjectLib(Integer.parseInt(args[0]), args[1], UN);
        Runtime.getRuntime().addShutdownHook(new CleanDirHelper(myId));
    }

    /**
     * process information
     * @param info Information
     */
    public void processMsg(Information info) {
        try {
            // just generate reply from info, and change the reply
            Information reply = new Information(info);
            // if a ASK msg
            if (info.action == Information.actionType.ASK) {
                System.out.println(myId + ": process ASK. txnid:" + info.txnID
                                    + " filename:" + info.filename + " comps:"
                                    + info.componentStr);
                String[] comps = info.componentStr.split("&");
                boolean ret = true;
                ArrayList<String> compList = new ArrayList<>();
                fileLock.writeLock().lock();
                for (String curtComp : comps) {
                    if (!imagesStatus.containsKey(curtComp)
                            || imagesStatus.get(curtComp) != 0) {
                        ret = false;
                    }
                    compList.add(curtComp);
                }
                if (ret) {
                    // ask user
                    boolean askRet = PL.askUser(info.img, comps);
                    if (askRet) {
                        lockFile(info.txnID, compList);
                        System.out.println(myId + ": process ASK. txnid:" + info.txnID +
                                            " filename:" + info.filename + " user accept");
                        reply.reply = true;
                        logHeader(info.txnID, info.filename, compList);
                        logEvent(info.txnID, "ACCEPT");
                    } else {
                        System.out.println(myId + ": process ASK. txnid:" + info.txnID +
                                            " filename:" + info.filename + " user refuse");
                        reply.reply = false;
                        logHeader(info.txnID, info.filename, compList);
                        logEvent(info.txnID, "REFUSE");
                    }
                } else {
                    System.out.println(myId + ": process ASK. txnid:" + info.txnID +
                                    " filename:" + info.filename +
                                    " file not exists or is locked");
                    reply.reply = false;
                    logHeader(info.txnID, info.filename, compList);
                    logEvent(info.txnID, "REFUSE");
                }
                fileLock.writeLock().unlock();
                sendMsg(reply, this.myId);
            // if a COMMIT msg
            } else if (info.action == Information.actionType.COMMIT) {
                fileLock.writeLock().lock();
                logEvent(info.txnID, "COMMIT");
                if (lockedFiles.containsKey(info.txnID) == false) {
                    // dup msg, resend
                    System.out.println(myId + ": receive dup COMMIT msg. txnid:"
                            + info.txnID + " filename:" + info.filename + "resend..");
                } else { // get this msg first time, process and send reply
                    System.out.println(myId + ": process COMMIT. txnid:" + info.txnID +
                            " filename:" + info.filename);
                    deleteFile(info.txnID);
                }
                fileLock.writeLock().unlock();
                sendMsg(reply, this.myId);
                logEvent(info.txnID, "FINISH");
            // if a ABORT msg
            } else if (info.action == Information.actionType.ABORT) {
                // might get duplicate commit msg.
                fileLock.writeLock().lock();
                logEvent(info.txnID, "ABORT");
                if (lockedFiles.containsKey(info.txnID) == false) {
                    System.out.println(myId + ": receive dup ABORT msg. txnid:" +
                            info.txnID + " filename:" + info.filename + "resend..");
                } else {
                    System.out.println(myId + ": process ABORT. txnid:" +
                            info.txnID + " filename:" + info.filename);
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

    /**
     * Lock related files to prevent future used by other transactions
     * @param txnid transactionID
     * @param comps components
     */
    public static void lockFile(int txnid, ArrayList<String> comps) {
        lockedFiles.put(txnid, comps);
        for (String curtComp : comps) {
            imagesStatus.put(curtComp, txnid);
        }
    }

    /**
     * Delete files from working directory.
     * @param txnid transactionID
     */
    public static void deleteFile(int txnid) {
        ArrayList<String> comps = lockedFiles.get(txnid);
        System.out.println(myId + ": txnid:" + txnid + " in delete. :"
                            + comps.toString());
        for (String curtComp : comps) {
            imagesStatus.remove(curtComp);
            File fileToDelete = new File(curtComp);
            fileToDelete.delete();
            System.out.println(myId + ": txnid:" + txnid +
                                " delete filename:" + curtComp);
            if (fileToDelete.exists()) {
                System.out.println("HELP!!!!!!!!!!!!");
            }
        }
        lockedFiles.remove(txnid);
    }

    /**
     * Release lock on locked files
     * @param txnid
     */
    public static void releaseHold(int txnid) {
        ArrayList<String> comps = lockedFiles.get(txnid);
        for (String curtComp : comps) {
            imagesStatus.put(curtComp, 0);
        }
        lockedFiles.remove(txnid);
    }


    /** send msg to Server.
     * usernode side don't have to keep track of whether server receive the msg.
     * b.c. if the msg is dropped, server will resend msg.
     * @param info information to send
     * @param myID user
     */
    public void sendMsg(Information info, String myID) {
        System.out.println(myID + ": send msg to server to " + info.action
                            + " txnid:" + info.txnID + " filename:" + info.filename
                            + "comps: " + info.componentStr + " reply:" + info.reply);
        ProjectLib.Message msg = new ProjectLib.Message("Server", info.getBytes());
        PL.sendMessage(msg);
    }

    /**
     * log event into this transaction's log file
     * @param txnID transactionID
     * @param event event
     * @throws Exception
     */
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


    /**
     * log transaction's information into "txnRecord.log"
     * @param txnID transactionID
     * @param filename filename
     * @param localSourceList source images List
     * @throws IOException
     */
    public static void logHeader(int txnID, String filename,
                                 ArrayList<String> localSourceList)
                                throws IOException {
        String logName = "txnRecord.log";
        // append
        FileWriter fos = new FileWriter(new File(logName), true);
        BufferedWriter osw = new BufferedWriter(fos);
        StringBuilder sb = new StringBuilder();
        sb.append(txnID).append(" ").append(filename).append(" ");
        for (int i = 0; i < localSourceList.size() - 1; ++i) {
            sb.append(localSourceList.get(i)).append(":");
            System.out.println(myId + ": in logHeader, txn:" + txnID +
                                " log component :" + localSourceList.get(i));
        }
        sb.append(localSourceList.get(localSourceList.size() - 1));
        System.out.println(myId + ": in logHeader, txn:" + txnID +
                                    " log component :" +
                                    localSourceList.get(localSourceList.size()-1));
        osw.write(sb.toString());
        osw.newLine();
        osw.flush();
        osw.close();
        PL.fsync();
    }


    /**
     * rollingback to last checkpoint
     * @throws Exception
     */
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

            System.out.println("rollingback -- txn:" + txnID + " " + status);
            // skip REFUSE and ABORT transaction
            if (status.equalsIgnoreCase("REFUSE")
                    || status.equalsIgnoreCase("ABORT")) {
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
                System.out.println(myId + ": in rollingback, txn:"+ txnID +
                                    " find component :" +
                                    localSourceList.get(localSourceList.size()-1));
            }
            localSourceList.add(components[components.length-1]);
            System.out.println(myId + ": in rollingback, txn:"+ txnID +
                                " find component :" +
                                localSourceList.get(localSourceList.size()-1));

            // lock "ACCEPT" txn's images
            if (status.equalsIgnoreCase("ACCEPT")) {

                System.out.println(myId + ": rollingback find "+ txnID +
                                    " accept, lock:" + localSourceList.toString());

                lockFile(txnID, localSourceList);
            //  redo "COMMIT" txns
            } else if (status.equalsIgnoreCase("COMMIT")) {

                System.out.println(myId + ": rollingback find "+ txnID +
                                " commit, delete files: " +
                                localSourceList.toString());

                lockFile(txnID, localSourceList);
                deleteFile(txnID);
            }

        } // end of a line

        fis.close();
        isw.close();
        recordFile.delete();
    }

    /**
     * clean working direcotry helper class
     */
    static class CleanDirHelper extends Thread{
        String id;
        CleanDirHelper(String id){this.id = id;}
        public void run() {
            // clean up dirs
            System.out.println(id+": dying.........");
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

