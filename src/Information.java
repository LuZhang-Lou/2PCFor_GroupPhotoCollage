import org.omg.PortableInterceptor.INACTIVE;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Created by Lu on 4/9/16.
 */
public class Information {
    public static enum actionType{ ASK, ABORT, COMMIT };
    actionType action;
    String filename;
    String node;
    String []component;
    byte[] img;
    boolean reply;

    Information(Information orig){
        this.action = orig.action;
        this.filename = orig.filename;
        this.node = orig.node;
        this.component = orig.component;
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

    Information(String actionStr, String filename, String node, String []component){
        this(actionStr);
        this.node = node;
        this.filename = filename;
        this.component = component;
        this.reply = false;
        this.img = new byte[1];
    }

    Information(String actionStr, String filename, String node, String[] component, byte[] img){
        this(actionStr, filename, node, component);
        this.img = img;
    }

    // will only get img[] when reveice ack of inquiry
    Information(byte[] bytes){
        String value = new String(bytes);
        int idx = value.indexOf("+");
        if (idx != -1) {
            value = value.substring(0, idx);
        }
        String[] parts = value.split(":");
        try{
        if (parts.length != 5) {
            throw new Exception("Error Bytes to construct Information");
        }
        }catch (Exception e){
            e.printStackTrace();
        }
        // action : filename : node: component : reply
        this.action = Information.valueOf(parts[0]);
        this.filename = parts[1];
        String node = parts[2];
        this.component = parts[3];
        this.reply = Boolean.valueOf(parts[4]);

        if (idx != -1) {
            this.img = new byte[bytes.length - idx];
            img = Arrays.copyOfRange(bytes, idx, bytes.length - 1);
        } else {
            img = new byte[1];
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
        return (this.action.equals(converted.action) && this.node.equals(converted.node) &&
                this.filename.equals(converted.filename) && this.component.equals(converted.component));
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
        sb.append(actionStr).append(":").append(this.filename).append(":").append(this.node).append(":").append(this.component).append(":").append(this.reply).append("+").append(img);
        return sb.toString();
    }

    public byte[] getBytes(){
        return this.toString().getBytes();
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
