import org.omg.PortableInterceptor.INACTIVE;

import java.io.FileOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by Lu on 4/9/16.
 */
public class Information {
    public static enum actionType{ ASK, ABORT, COMMIT };
    actionType action;
    int txnID;
    String filename;
    String node;
//    ArrayList<String> components;
    String componentStr;
    byte[] img;
    boolean reply;

    Information(Information orig){
        this.txnID = orig.txnID;
        this.action = orig.action;
        this.filename = orig.filename;
        this.node = orig.node;
        this.componentStr = orig.componentStr;
        this.img = null;
    }

    Information (String actionStr){
        try {
            if (actionStr.equalsIgnoreCase("ASK")) {
                this.action = actionType.ASK;
            } else if (actionStr.equalsIgnoreCase("ABORT")) {
                this.action = actionType.ABORT;
            } else if (actionStr.equalsIgnoreCase("COMMIT")) {
                this.action = actionType.COMMIT;
            } else {
                throw new Exception("Invalid actionTpye");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    Information(int id, String actionStr, String filename, String node){
        this(actionStr);
        this.txnID = id;
        this.filename = filename;
        this.node = node;
        this.img = null;
        this.componentStr = null;
    }


    Information(int id, String actionStr, String filename, String node, String componentStr){
        this(id, actionStr, filename, node);
        this.reply = false;
        this.img = null;
        this.componentStr = componentStr;
    }


    Information(int id, String actionStr, String filename, String node, ArrayList<String> components, byte[] img){
//        this(actionStr, filename, node, components);
        this(id, actionStr, filename, node);
        this.reply = false;
        this.img = null;

//        Collections.sort(components);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < components.size(); ++i){
            String curtComp = components.get(i);
            if (i == 0)
                sb.append(curtComp);
           else
                sb.append("&").append(curtComp);
        }
        this.componentStr = sb.toString();
//        System.out.println("============================== id:" + id + " of node: " + node + " with: " + this.componentStr);
        this.img = img;
    }

    // will only get img[] when reveice ack of inquiry
    // deserialization.
    Information(byte[] bytes){
        int plusIdx = 0;
        //todo: hardcode 100
        for (plusIdx = 0; plusIdx < bytes.length; ++plusIdx){
            if(bytes[plusIdx] == 43){ // plus
                break;
            }
        }

        String value = new String(bytes);
//        System.out.println("init Information from byte[] : " + value);
        String [] parts = null;
        try{
//            System.out.println("idx pos:" + plusIdx);
            if (plusIdx == bytes.length-1) { // img is null
//                System.out.println("******************BAOJING!!!!!!!!!");
                value = value.substring(0, plusIdx);
            }
            parts = value.split(":");
            if (parts.length < 6) {
                System.out.print(value);
                throw new Exception("Error Bytes to construct Information");
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        // action : filename : node: component : reply
        this.txnID = Integer.parseInt(parts[0]);
        this.action = Information.valueOf(parts[1]);
        this.filename = parts[2];
        this.node = parts[3];
        this.componentStr = parts[4];
        this.reply = Boolean.valueOf(parts[5]);

        //if (plusIdx != bytes.length ) {
        // note: can't equal! then img is null!
        if ((plusIdx+1) <= bytes.length-1){
            this.img = new byte[bytes.length - (plusIdx+1)];
            this.img = Arrays.copyOfRange(bytes, plusIdx+1, bytes.length);
            System.out.println("LOOKHERE2..." + img.length);
        } else {
            this.img = null;
        }

    }

    @Override
    public int hashCode() {
        return this.filename.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj==null) return false;
        if (!(obj instanceof Information))
            return false;
        if (obj == this)
            return true;
        Information converted = (Information) obj;

        // don't need to consider the equality of reply.
        // todo: assume filename is unique
        return (this.action.equals(converted.action) && this.node.equals(converted.node) &&
                this.filename.equals(converted.filename) && (this.txnID == converted.txnID));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String actionStr = null;
        if (this.action == actionType.ASK){
            actionStr = "ASK";
        }else if (this.action == actionType.ABORT){
            actionStr = "ABORT";
        } else {
            actionStr = "COMMIT";
        }
        // action : filename : node: component : reply + byte[] img
        sb.append(this.txnID).append(":").append(actionStr).append(":").append(this.filename).append(":");
        sb.append(this.node).append(":");
        sb.append(componentStr).append(":").append(this.reply).append("+");
//        if (this.img != null){
//            sb.append("+").append(img);
//        }
        return sb.toString();
    }

    public byte[] getBytes(){
        byte[] tmp = this.toString().getBytes();
        byte[] ret = tmp;
        if (img != null) {
            int lenSum = tmp.length + this.img.length;
            ret = new byte[lenSum];
            System.arraycopy(tmp, 0, ret, 0, tmp.length);
            System.arraycopy(this.img, 0, ret, tmp.length, this.img.length);
        }
        return ret;
    }

    public static actionType valueOf(String actionStr){
        if (actionStr.equalsIgnoreCase("ASK")) {
            return actionType.ASK;
        }else if (actionStr.equalsIgnoreCase("ABORT")) {
            return actionType.ABORT;
        } else{
            return actionType.COMMIT;
        }

    }



}
