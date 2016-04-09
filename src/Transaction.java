import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Lu on 4/8/16.
 */
public class Transaction {

    public enum TXNStatus {ONGOING, FAILED, COMMIT};
    public String filename;
    public TXNStatus status;
    public AtomicInteger consensusCnt;
    public int nodeCnt;

    // key: node:component, value : the answer
    public ConcurrentHashMap<Source, Boolean> answerList;
    Transaction(String filename, String[] nodes, String[] components){
        this.filename = filename;
        this.status = TXNStatus.ONGOING;
        consensusCnt = new AtomicInteger(0);
        this.answerList = new ConcurrentHashMap<>();
        this.nodeCnt = nodes.length;

        for (int i = 0; i < nodes.length; i++){
            String n = nodes[i];
            String c = components[i];
        }

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
