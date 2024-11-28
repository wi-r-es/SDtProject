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

    private String nodeName;
    private UUID nodeId;
    private int udpPort;

    // Updated constructor with node information
    public Message(OPERATION op, Object pl, String nodeName, UUID nodeId, int udpPort) {
        this.payload = pl;
        this.header = op;
        this.timestamp = System.currentTimeMillis();
        this.nodeName = nodeName;
        this.nodeId = nodeId;
        this.udpPort = udpPort;
    }
    

    // Backward compatibility constructor
    public Message(OPERATION op, Object pl) {
        this(op, pl, null, null, 0);
    }
    // public Message(OPERATION op, Object pl){
    //     this.payload = pl;
    //     this.header =op;
    //     this.timestamp = System.currentTimeMillis(); 
    // }
    public String getNodeName() {
        return nodeName;
    }

    public UUID getNodeId() {
        return nodeId;
    }

    public int getUdpPort() {
        return udpPort;
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
    // @Override
    // public String toString() {
    //     return "Message{header='" + header + "', payload=" + payload + "}";
    // }
    @Override
    public String toString() {
        return "Message{" +
               "header='" + header + "'" +
               ", payload=" + payload +
               ", nodeName='" + nodeName + "'" +
               ", nodeId=" + nodeId +
               ", udpPort=" + udpPort +
               "}";
    }

    public static Message heartbeatMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.HEARTBEAT, content, nodeName, nodeId, udpPort);
    }
    public static Message heartbeatMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.HEARTBEAT, PAYLOAD);
    }

    public static Message LheartbeatMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.LHEARTBEAT, content, nodeName, nodeId, udpPort);
    }

    public static Message replyHeartbeatMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.HEARTBEAT_ACK, content, nodeName, nodeId, udpPort);
    }

    public static Message SyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.SYNC, content, nodeName, nodeId, udpPort);
    }

    public static Message FullSyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.FULL_SYNC, content, nodeName, nodeId, udpPort);
    }

    public static Message replySyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.ACK, content, nodeName, nodeId, udpPort);
    }

    public static Message replyFullSyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.FULL_SYNC_ACK, content, nodeName, nodeId, udpPort);
    }

    public static Message discoveryMessage(UUID nodeId, int port, String nodeName) {
        String PAYLOAD = "WHOS_THE_LEADER:" + nodeId + ":" + port + ":" + System.currentTimeMillis();
        return new Message(OPERATION.DISCOVERY, PAYLOAD, nodeName, nodeId, port);
    }

    public static Message replyDiscoveryMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.DISCOVERY_ACK, content, nodeName, nodeId, udpPort);
    }
    
    
    @Deprecated
    public static Message LheartbeatMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.LHEARTBEAT, PAYLOAD);
    }
    @Deprecated
    public static Message replyHeartbeatMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.HEARTBEAT_ACK, PAYLOAD);
    }
    @Deprecated
    public static Message SyncMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.SYNC, PAYLOAD);
    }
    @Deprecated
    public static Message FullSyncMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.FULL_SYNC, PAYLOAD);
    }
    @Deprecated
    public static Message replySyncMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.ACK, PAYLOAD);
    }
    @Deprecated
    public static Message replyFullSyncMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.FULL_SYNC_ACK, PAYLOAD);
    }
    @Deprecated
    public static Message discoveryMessage(UUID nodeId, int port){
        String PAYLOAD = "WHOS_THE_LEADER:" + nodeId + ":" + port + ":" + System.currentTimeMillis();
        return new Message(OPERATION.DISCOVERY, PAYLOAD);
    }
    @Deprecated
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