import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Lu on 4/8/16.
 */
public class Transaction {

    public enum TXNStatus {INQUIRY, ABORT, COMMIT};
    public String filename;
    public TXNStatus status;
    public AtomicInteger consensusCnt;
    public int nodeCnt;
    public byte[] img;
    public int txnId;
    // key: node:component, value : the answer
    public ConcurrentHashMap<Source, Boolean> answerList;

    Transaction(int id, String filename, byte[] img, int num){
        this.txnId = id;
        this.filename = filename;
        this.status = TXNStatus.INQUIRY;
        consensusCnt = new AtomicInteger(0);
        this.answerList = new ConcurrentHashMap<>();
        this.nodeCnt = num;
        this.img = img;
    }

    public static class Source{
        String node;
        String component;
        Source(String node, String component){
            this.node = node;
            this.component = component;
        }

        @Override
        public int hashCode() {
            return this.node.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if(obj==null) return false;
            if (!(obj instanceof Source))
                return false;
            if (obj == this)
                return true;
            Source converted = (Source) obj;
            return (this.node.equals(converted.node) && this.component.equals(converted.component));
        }
    }
}
