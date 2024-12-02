package shared;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Message class represents a message that can be sent between nodes in a distributed system.
 * It implements the Serializable interface to allow messages to be serialized and deserialized.
 */
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

    /**
     * Constructs a new Message object with the specified operation, payload, node name, node ID, and UDP port.
     *
     * @param op       The operation type of the message.
     * @param pl       The payload of the message.
     * @param nodeName The name of the node sending the message.
     * @param nodeId   The UUID of the node sending the message.
     * @param udpPort  The UDP port of the node sending the message.
     */
    public Message(OPERATION op, Object pl, String nodeName, UUID nodeId, int udpPort) {
        this.payload = pl;
        this.header = op;
        this.timestamp = System.currentTimeMillis();
        this.nodeName = nodeName;
        this.nodeId = nodeId;
        this.udpPort = udpPort;
    }
    

    /**
     * Constructs a new Message object with the specified operation and payload.
     * This constructor is provided for backward compatibility.
     *
     * @param op The operation type of the message.
     * @param pl The payload of the message.
     */
    public Message(OPERATION op, Object pl) {
        this(op, pl, null, null, 0);
    }
    // public Message(OPERATION op, Object pl){
    //     this.payload = pl;
    //     this.header =op;
    //     this.timestamp = System.currentTimeMillis(); 
    // }


    /**
     * Returns the name of the node sending the message.
     *
     * @return The name of the node sending the message.
     */
    public String getNodeName() {
        return nodeName;
    }
    /**
     * Returns the UUID of the node sending the message.
     *
     * @return The UUID of the node sending the message.
     */
    public UUID getNodeId() {
        return nodeId;
    }
     /**
     * Returns the UDP port of the node sending the message.
     *
     * @return The UDP port of the node sending the message.
     */
    public int getUdpPort() {
        return udpPort;
    }

    /**
     * Returns the operation type of the message.
     *
     * @return The operation type of the message.
     */
    public OPERATION getOperation(){
        return header;
    }
    /**
     * Returns the payload of the message.
     *
     * @return The payload of the message.
     */
    public Object getPayload(){
        return payload;
    }
     /**
     * Returns the unique message ID.
     *
     * @return The unique message ID.
     */
    public int getMessageId(){
        return message_id.get();
    }
    /**
     * Returns the timestamp of the message.
     *
     * @return The timestamp of the message.
     */
    public long getTimestamp(){
        return this.timestamp;
    }
    // @Override
    // public String toString() {
    //     return "Message{header='" + header + "', payload=" + payload + "}";
    // }

    /**
     * Returns a string representation of the Message object.
     *
     * @return A string representation of the Message object.
     */
    @Override
    public String toString() {
        return "Message{" +
               "header='" + header + "'" +
               ", payload=" + payload.toString() +
               ", nodeName='" + nodeName + "'" +
               ", nodeId=" + nodeId +
               ", udpPort=" + udpPort +
               "}";
    }
    /**
     * Creates a new heartbeat message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the heartbeat message.
     * @param nodeName The name of the node sending the heartbeat message.
     * @param nodeId   The UUID of the node sending the heartbeat message.
     * @param udpPort  The UDP port of the node sending the heartbeat message.
     * @return A new heartbeat message.
     */
    public static Message heartbeatMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.HEARTBEAT, content, nodeName, nodeId, udpPort);
    }
    /**
     * Creates a new heartbeat message with the specified content.
     *
     * @param content The content of the heartbeat message.
     * @return A new heartbeat message.
     */
    public static Message heartbeatMessage(String content){
        String PAYLOAD = content;
        return new Message(OPERATION.HEARTBEAT, PAYLOAD);
    }
    /**
     * Creates a new leader heartbeat message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the leader heartbeat message.
     * @param nodeName The name of the node sending the leader heartbeat message.
     * @param nodeId   The UUID of the node sending the leader heartbeat message.
     * @param udpPort  The UDP port of the node sending the leader heartbeat message.
     * @return A new leader heartbeat message.
     */
    public static Message LheartbeatMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.LHEARTBEAT, content, nodeName, nodeId, udpPort);
    }
    /**
     * Creates a new heartbeat acknowledgment message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the heartbeat acknowledgment message.
     * @param nodeName The name of the node sending the heartbeat acknowledgment message.
     * @param nodeId   The UUID of the node sending the heartbeat acknowledgment message.
     * @param udpPort  The UDP port of the node sending the heartbeat acknowledgment message.
     * @return A new heartbeat acknowledgment message.
     */
    public static Message replyHeartbeatMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.HEARTBEAT_ACK, content, nodeName, nodeId, udpPort);
    }
    /**
     * Creates a new sync message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the sync message.
     * @param nodeName The name of the node sending the sync message.
     * @param nodeId   The UUID of the node sending the sync message.
     * @param udpPort  The UDP port of the node sending the sync message.
     * @return A new sync message.
     */
    public static Message SyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.SYNC, content, nodeName, nodeId, udpPort);
    }
    /**
     * Creates a new full sync message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the full sync message.
     * @param nodeName The name of the node sending the full sync message.
     * @param nodeId   The UUID of the node sending the full sync message.
     * @param udpPort  The UDP port of the node sending the full sync message.
     * @return A new full sync message.
     */
    public static Message FullSyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.FULL_SYNC, content, nodeName, nodeId, udpPort);
    }
    /**
     * Creates a new sync acknowledgment message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the sync acknowledgment message.
     * @param nodeName The name of the node sending the sync acknowledgment message.
     * @param nodeId   The UUID of the node sending the sync acknowledgment message.
     * @param udpPort  The UDP port of the node sending the sync acknowledgment message.
     * @return A new sync acknowledgment message.
     */
    public static Message replySyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.ACK, content, nodeName, nodeId, udpPort);
    }
    /**
     * Creates a new full sync acknowledgment message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the full sync acknowledgment message.
     * @param nodeName The name of the node sending the full sync acknowledgment message.
     * @param nodeId   The UUID of the node sending the full sync acknowledgment message.
     * @param udpPort  The UDP port of the node sending the full sync acknowledgment message.
     * @return A new full sync acknowledgment message.
     */
    public static Message replyFullSyncMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        return new Message(OPERATION.FULL_SYNC_ACK, content, nodeName, nodeId, udpPort);
    }
    /**
     * Creates a new discovery message with the specified node ID, port, and node name.
     *
     * @param nodeId   The UUID of the node sending the discovery message.
     * @param port     The port of the node sending the discovery message.
     * @param nodeName The name of the node sending the discovery message.
     * @return A new discovery message.
     */
    public static Message discoveryMessage(UUID nodeId, int port, String nodeName) {
        String PAYLOAD = "WHOS_THE_LEADER:" + nodeId + ":" + port + ":" + System.currentTimeMillis();
        return new Message(OPERATION.DISCOVERY, PAYLOAD, nodeName, nodeId, port);
    }
    /**
     * Creates a new discovery acknowledgment message with the specified content, node name, node ID, and UDP port.
     *
     * @param content  The content of the discovery acknowledgment message.
     * @param nodeName The name of the node sending the discovery acknowledgment message.
     * @param nodeId   The UUID of the node sending the discovery acknowledgment message.
     * @param udpPort  The UDP port of the node sending the discovery acknowledgment message.
     * @return A new discovery acknowledgment message.
     */
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