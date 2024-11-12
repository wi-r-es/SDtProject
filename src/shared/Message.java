package shared;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class Message implements Serializable{
    private final int node_id;
    private final AtomicInteger message_id = new AtomicInteger(0);
    private final String message;
    private final long timestamp;

    public Message(int node_id, String message, long time){
        this.node_id = node_id;
        this.message=message;
        this.timestamp = time;
        message_id.getAndIncrement();
    }

    public String getMessage(){
        return message;
    }

    public int getNodeId(){
        return node_id;
    }

    public int getMessageId(){
        return message_id.get();
    }
    public long getTimestamp(){
        return this.timestamp;
    }
}
