import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Lu on 4/8/16.
 */
public class Transaction {

    public enum TXNStatus {ONGOING, FAILED, COMMIT};
    public TXNStatus status;
    public int id;
    public AtomicInteger consensusCnt;
    public ConcurrentHashMap<String, AtomicBoolean> replyList;
    Transaction(int id, String[] nodes){
        this.status = TXNStatus.ONGOING;
        this.id = id;
        consensusCnt = new AtomicInteger(0);
        this.replyList = new ConcurrentHashMap<>();
        for (String node: nodes){
            replyList.put(node, new AtomicBoolean(false));
        }
    }
}
