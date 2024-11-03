import java.util.HashMap;

 class Element {
    protected int lider;
    private SendTransmitter sender;
    private ReceiveListener receiver;
    private static HashMap<Integer, String> messagesLists = new HashMap<>();

    public Element(int lider) {
        this.lider = lider;
        this.sender = new SendTransmitter();
        this.receiver = new ReceiveListener();
    }

    public void start() {
        // Start sender only if this is the leader
        if (lider == 1) {
            new Thread(sender).start();
        }
        // All elements receive messages
        new Thread(receiver).start();
    }

    public void addMessage(Integer key, String message) {
        if (lider == 1) {  // Only leader can add messages
            messagesLists.put(key, message);
        }
    }

    // Getter for messages list
    public static HashMap<Integer, String> getMessagesLists() {
        return messagesLists;
    }
}
