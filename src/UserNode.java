import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class UserNode implements ProjectLib.MessageHandling {
    public ConcurrentHashMap<String, AtomicBoolean> imagesStatus;
    public static ConcurrentHashMap<Information, AtomicBoolean> globalReplyList = new ConcurrentHashMap<>();
	public final String myId;
    public static ProjectLib PL;
    public static UserNode UN;
	public UserNode( String id ) {
		myId = id;
        imagesStatus = new ConcurrentHashMap<>();

        final File home = new File(".");
        for (final File fileEntry : home.listFiles()) {
            if (!fileEntry.isDirectory() && fileEntry.getName().endsWith(".jpg")) {
                System.out.println("UserNode ID:" + myId + " init file:" + fileEntry.getName());
                imagesStatus.put(fileEntry.getName(), new AtomicBoolean(true));
            }
        }

	}

	public boolean deliverMessage( ProjectLib.Message msg ) {
		System.out.println( myId + ": Got message from " + msg.addr );
        Information info = new Information(msg.body);
        processMsg(info);
		return true;
	}
	
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );

//        PL.askUser()

	}

    public void processMsg(Information info){
        Information reply = new Information(info);
        if (info.action == Information.actionType.ASK){
            if (imagesStatus.containsKey(info.component) && PL.askUser(info.img, )){
                boolean ret = imagesStatus.get(info.component).compareAndSet(true, false);
                if (ret) {
                    reply.reply = true;
                }else {
                    reply.reply = false;
                }

            }else {
                reply.reply = false;
            }
            blockingSendMsg(reply);
        } else if (info.action == Information.actionType.COMMIT){
            //imageStatus.get(info.component) must be false;
            imagesStatus.remove(info.component);
            File imageToDelete = new File(info.component);
            imageToDelete.delete();
        } else if (info.action == Information.actionType.ABORT){
            //imageStatus.get(info.component) must be false;
            imagesStatus.get(info.component).set(true);
        }
    }




    public static void blockingSendMsg(Information info){
        new Thread(new Runnable() {
            @Override
            public void run() {
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

