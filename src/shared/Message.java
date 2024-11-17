package shared;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import Resources.Document;

public class Message implements Serializable{
    // 
    private static final long serialVersionUID = 1L;
    private final OPERATION header;
    private Object payload;
    private final AtomicInteger message_id = new AtomicInteger(0);
    private final long timestamp;
    

    public Message(OPERATION op, Object pl){
        this.payload = pl;
        this.header =op;
        this.timestamp = System.currentTimeMillis(); 
    }

    public OPERATION getOperation(){
        return header;
    }

    public Object getPayload(){
        return payload;
    }

    public int getMessageId(){
        return message_id.get();
    }
    
    public long getTimestamp(){
        return this.timestamp;
    }
    @Override
    public String toString() {
        return "Message{header='" + header + "', payload=" + payload + "}";
    }



    public static Message heartbeatMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.HEARTBEAT, PAYLOAD);
    }
    public static Message replyHeartbeatMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.HEARTBEAT_ACK, PAYLOAD);
    }
    public static Message SyncMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.SYNC, PAYLOAD);
    }
    public static Message replySyncMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.ACK, PAYLOAD);
    }
    public static Message discoveryMessage(UUID nodeId, int port){
        String PAYLOAD = "WHOS_THE_LEADER:" + nodeId + ":" + port + ":" + System.currentTimeMillis();
        return new Message(OPERATION.DISCOVERY, PAYLOAD);
    }
    public static Message replyDiscoveryMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.DISCOVERY_ACK, PAYLOAD);
    }
    
}


//private class MessagePayload implements Serializable{
    //     private final Document doc;
    //     private final String msg;
        

    //     public MessagePayload(Document document){
    //         this.doc = document;
            
    //         message_id.getAndIncrement();
    //         msg = null;
    //     }
    //     public MessagePayload(String msg){
    //         this.doc = null;
    //         this.timestamp = System.currentTimeMillis();
    //         message_id.getAndIncrement();
    //         this.msg = msg;
    //     }

    //     public Document getDocument(){
    //         return doc;
    //     }
    //     public int getMessageId(){
    //         return message_id.get();
    //     }
    //     public long getTimestamp(){
    //         return this.timestamp;
    //     }

    //     public String getMSG(){
    //         return this.msg;
    //     }

    //     @Override
    //     public String toString(){
    //         return "Document{id='" + doc.getId() + "', content='" + doc.getContent() +  "', version='" + doc.getVersion() + "'}";
    //     }

    // }