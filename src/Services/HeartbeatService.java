package Services;
import Nodes.*;
import Nodes.Raft.AppendEntriesArgs;
import Nodes.Raft.AppendEntriesReply;
import Nodes.Raft.LogEntry;
import Nodes.Raft.NodeState;
import Nodes.Raft.RaftNode;
import Nodes.Raft.RequestVoteArgs;
import Nodes.Raft.RequestVoteReply;
import Resources.Document;
import shared.Message;
import shared.OPERATION;
import utils.CompressionUtils;
import utils.network;
import java.io.IOException;

import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * The HeartbeatService class handles heartbeat broadcasting and failure detection for a (Gossip)Node.
 * It extends the Thread class to run as a separate thread.
 * @see Nodes.GossipNode
 * @see ConcurrentHashMap
 * @see ScheduledExecutorService
 */
@SuppressWarnings("unused")
public class HeartbeatService extends Thread {
    //CONSTANTS
    private static final int HEARTBEAT_INTERVAL = 5000;  // Interval in milliseconds for sending heartbeats
    private static final int FAILURE_TIMEOUT = 5000;  // Timeout to detect failure (ms)
    private static final int NODE_PORT_BASE = 9678;  // base port for UDP communication
    private static final int PORT = 9876;  // UDP communication multicast
    private static final String MULTICAST_GROUP = "230.0.0.0";  
    private final int TCP_PORT = 9090;//for ack syncs
    //INSTANCE VARIABLES
    private final GossipNode gossipNode;
    private final Map<UUID, AtomicInteger> heartbeatCounters;  // heartbeat counter for each node
    private final Map<String, Long> lastReceivedHeartbeats;  // last received heartbeat timestamps
    private final ScheduledExecutorService scheduler; // for running heartbeats regularly [and fail detection in the future]
    private DatagramSocket socket;
    private int udpPort;

    private static Set<Integer> usedPorts = new HashSet<>();
    private final int MIN_PORT = 49152;  
    private final int MAX_PORT = 65535;  

    // For log replication fail for testing purposes
    private volatile boolean isSuspended = false;
    public void HBsuspend() {
        this.isSuspended = true;
        System.out.println("[DEBUG] HeartbeatService suspended for node " + gossipNode.getNodeName());
    }
    public void HBresume() {
        this.isSuspended = false;
        System.out.println("[DEBUG] HeartbeatService resumed for node " + gossipNode.getNodeName());
    }

    /**
     * Constructor for the HeartbeatService class.
     *
     * @param node The GossipNode associated with this HeartbeatService.
     * @see Executors#newScheduledThreadPool()
     */
    public HeartbeatService(GossipNode node) {
        this.gossipNode = node;
        this.heartbeatCounters = new ConcurrentHashMap<>();
        this.lastReceivedHeartbeats = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2); //one for heartbeat one for fail detection

        try {
            this.udpPort = NODE_PORT_BASE + Math.abs(gossipNode.getNodeId().hashCode()) % 1000;
            this.socket = new DatagramSocket(udpPort);  // Unique port 
            //this.socket.setSoTimeout(HEARTBEAT_INTERVAL);
        } catch (SocketException e) {
            e.printStackTrace();
        }

        heartbeatCounters.put(gossipNode.getNodeId(), new AtomicInteger(0));  // Initializes heartbeat counter
    }
    /**
     * Returns a string representation of the HeartbeatService.
     *
     * @return A string containing the node name and UDP port.
     */
    @Override
    public String toString(){
        return "Node{id='" + gossipNode.getNodeName()  +  "', port='" + this.getUDPport() + "'}";
    }
    /**
     * Returns the UDP port used by the HeartbeatService.
     *
     * @return The UDP port number.
     */
    public int getUDPport(){
        return this.udpPort;
    }

    
    /**
     * The run method is executed when the thread starts.
     * It schedules heartbeat broadcasting, failure detection and starts separate threads for receiving heartbeats and messages.
     * @see Services.HeartbeatService#incrementAndBroadcastHeartbeat()
     * @see Services.HeartbeatService#detectFailures()
     * @see Services.HeartbeatService#receiveMessage()
     * @see ScheduledExecutorService#scheduleAtFixedRate()
     */
    @Override
    public void run() {        // Start heartbeat incrementing and failure detection tasks using scheduler
        // broadcast way
        scheduler.scheduleAtFixedRate(this::incrementAndBroadcastHeartbeat, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);

        // Detection failure
        scheduler.scheduleAtFixedRate(this::detectFailures, 0, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);

        // Start a separate thread for continuously receiving heartbeats
        Thread multicastThread = new Thread(() -> {
            while (gossipNode.isRunning() && !Thread.currentThread().isInterrupted()) {
                try {
                    receiveMulticast();
                } catch (Exception e) {
                    if (!gossipNode.isRunning()) break; // Exit if service is stopping
                    if (!(e instanceof SocketException)) { // Only log non-socket closure errors
                        System.err.println("Error in multicast receive: " + e.getMessage());
                    }
                }
            }
        });
        // Start message receiving threads
        Thread messageThread = new Thread(() -> {
            while (gossipNode.isRunning() && !Thread.currentThread().isInterrupted()) {
                try {
                    receiveMessage();
                } catch (Exception e) {
                    if (!gossipNode.isRunning()) break; // Exit if service is stopping
                    if (!(e instanceof SocketException)) { // Only log non-socket closure errors
                        System.err.println("Error in message receive: " + e.getMessage());
                    }
                }
            }
        });

        multicastThread.start();
        messageThread.start();

    }
    
    //Broadcast implementation
    /**
     * Increments the heartbeat counter and broadcasts a heartbeat message.
     * @see Services.HeartbeatService#broadcast()
     */
    public void incrementAndBroadcastHeartbeat() {
        {
            Message hb_message = Message.heartbeatMessage("Heartbeat:" + gossipNode.getNodeId() + ":" + this.udpPort + ":" + MULTICAST_GROUP + ":" + incrementHeartbeat()); 
            //Message hb_message = Message.LheartbeatMessage("Heartbeat from leader:" + gossipNode.getRaftNode().getNodeId() + ":" + gossipNode.getRaftNode().getCurrentTerm());
            this.broadcast(hb_message, false);
        }

    }
    /**
     * Returns the current heartbeat counter value for the associated GossipNode.
     *
     * @return The current heartbeat counter value.
     */
    private int getHeartbeatCounter() {
        return heartbeatCounters.get(gossipNode.getNodeId()).get();
    }
    /**
     * Increments the heartbeat counter for the associated GossipNode.
     *
     * @return The incremented heartbeat counter value.
     */
    private int incrementHeartbeat() {
        return heartbeatCounters.get(gossipNode.getNodeId()).incrementAndGet();
    }


/*
██████  ██████   ██████   █████  ██████   ██████  █████  ███████ ████████ 
██   ██ ██   ██ ██    ██ ██   ██ ██   ██ ██      ██   ██ ██         ██    
██████  ██████  ██    ██ ███████ ██   ██ ██      ███████ ███████    ██    
██   ██ ██   ██ ██    ██ ██   ██ ██   ██ ██      ██   ██      ██    ██    
██████  ██   ██  ██████  ██   ██ ██████   ██████ ██   ██ ███████    ██    
                                                                        
 */
    //BROADCAST
    /**
     * Broadcasts a message to all nodes in the network.
     *
     * @param message  The message to broadcast.
     * @param compress Indicates whether the message should be compressed before broadcasting.
     * @see network#serialize()
     * @see network#addHeader()
     */
    public void broadcast(Message message, boolean compress) {
        try (MulticastSocket multicastSocket = new MulticastSocket()) {

            byte[] serializedData = network.serialize(message);
    
            byte[] finalData;
            if (compress) {
                byte[] compressedData = CompressionUtils.compress(serializedData);
                finalData = network.addHeader("COMP", compressedData);
            } else {
                finalData = network.addHeader("UNCO", serializedData);
            }
    
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, group, PORT);
            multicastSocket.send(packet);
    
            // System.out.println("Broadcasting message: " + message.getOperation() + " with compression=" + compress + " content=[" +  "]");
            // System.out.println("Broadcasting message content: " + message.getPayload() + "]");
        } catch (IOException e) {
            System.err.println("Error broadcasting message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /*
                                    ██   ██ ███████  █████  ██████  ████████ ██████  ███████  █████  ████████ 
                                    ██   ██ ██      ██   ██ ██   ██    ██    ██   ██ ██      ██   ██    ██    
                                    ███████ █████   ███████ ██████     ██    ██████  █████   ███████    ██    
                                    ██   ██ ██      ██   ██ ██   ██    ██    ██   ██ ██      ██   ██    ██    
                                    ██   ██ ███████ ██   ██ ██   ██    ██    ██████  ███████ ██   ██    ██                                                                                                                                                                                
     */
    /**
    * Responds to a heartbeat from a target node.
    *
    * @param targetNodeId The UUID of the target node.
    * @param target_port  The port of the target node.
    * @see Services.HeartbeatService#sendUncompMessage()
    * @see shared.Message#replyHeartbeatMessage(String)
    */
    @SuppressWarnings("deprecation")
    private void respondeToHeartbeat(UUID targetNodeId, int target_port) {
        Message msg = Message.replyHeartbeatMessage("ACK_Heartbeat:" + gossipNode.getNodeId() + ":" + this.udpPort + ":"  
                                    + heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement() + ":" + System.currentTimeMillis());
        sendUncompMessage(msg, targetNodeId, target_port);

        //System.out.println("ACK PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
    }
    /**
    * Processes a received heartbeat message and replies to the sender.
    *
    * @param message The received heartbeat message.
    * @see Services.HeartbeatService#respondeToHeartbeat()
    * @see shared.Message#getPayload()
    */
    @SuppressWarnings("unused")
    private void replyToHeartbeat(Message message){
        Object obj = message.getPayload();
        String content = (String) obj;
        //System.out.println("\tCotent inside reply to heartbeat->  " + content+"\n\n");
        String[] parts = content.split(":");
        String senderNodeId = parts[1];
        int target_port = Integer.parseInt(parts[2]);
        if(gossipNode.getNodeId().equals(UUID.fromString(senderNodeId))) { return;}
        int heartbeatCounter = Integer.parseInt(parts[4]);

        // Update local heartbeat data

        //computeIfAbsente looks for an entry in "heartbeatCounters" with the key senderNodeId.
        // if key not present, creates a nwe entry with amoticInteger initialization with initialValue = 0
        // updateandGet will atomically updated the value retrieved from before 
        
        heartbeatCounters.computeIfAbsent(UUID.fromString(senderNodeId), k -> new AtomicInteger(0))
            .updateAndGet(current -> Math.max(current, heartbeatCounter));
        lastReceivedHeartbeats.put(senderNodeId, System.currentTimeMillis());
        
        respondeToHeartbeat(UUID.fromString(senderNodeId), target_port );        
    }
    /**
    * Adds a known node to the GossipNode's list of known nodes.
    *
    * @param message The message containing the node information.
    * @see Nodes.GossipNode#addKnownNode(UUID)
    * @see shared.Message#getPayload()
    */
    private void addKnownNode(Message message){
        Object obj = message.getPayload();
        String content = (String) obj;
        //System.out.println("\tCotent inside add to knownNodes->  " + content+"\n\n");
        String[] parts = content.split(":");
        String senderNodeId = parts[1];
        int port = Integer.parseInt(parts[2]);

        gossipNode.addKnownNode(UUID.fromString(senderNodeId), port);
    }

    /*
██████  ███████  ██████ ███████ ██ ██    ██ ███████ ███    ███ ███████ ███████ ███████  █████   ██████  ███████ 
██   ██ ██      ██      ██      ██ ██    ██ ██      ████  ████ ██      ██      ██      ██   ██ ██       ██      
██████  █████   ██      █████   ██ ██    ██ █████   ██ ████ ██ █████   ███████ ███████ ███████ ██   ███ █████   
██   ██ ██      ██      ██      ██  ██  ██  ██      ██  ██  ██ ██           ██      ██ ██   ██ ██    ██ ██      
██   ██ ███████  ██████ ███████ ██   ████   ███████ ██      ██ ███████ ███████ ███████ ██   ██  ██████  ███████ 
                                                                                                                
███    ███ ██    ██ ██      ████████ ██  ██████  █████  ███████ ████████ 
████  ████ ██    ██ ██         ██    ██ ██      ██   ██ ██         ██    
██ ████ ██ ██    ██ ██         ██    ██ ██      ███████ ███████    ██    
██  ██  ██ ██    ██ ██         ██    ██ ██      ██   ██      ██    ██    
██      ██  ██████  ███████    ██    ██  ██████ ██   ██ ███████    ██     
                                                                                                                                   
     */
    
    /**
     * Receives multicast messages and processes them based on the operation type.
     * Listens on a multicast group and handles incoming messages.
     * Messages can be compressed or uncompressed.
     * Compressed messages are decompressed before processing.
     * Supported operations:
     * - SYNC: Processes a sync request if the node is not the leader.
     * - REVERT: Reverts changes in the node's documents if the node is not the leader.
     * - HEARTBEAT: Replies to a heartbeat message.
     * - LHEARTBEAT: Handles a leader heartbeat message.
     * - COMMIT: Processes a commit operation.
     * - DISCOVERY: Processes a discovery request if the node is the leader.
     * - VOTE_REQ: Handles a vote request if the node is not the candidate.
     * - FULL_SYNC: Handles a full sync request if the node is the leader.
     * Unsupported operations are logged as errors.
     * 
     * @see shared.OPERATION
     * @see utils.CompressionUtils#decompress()
     * @see utils.network#deserialize()
     * @see shared.Message#getOperation()
     * @see shared.Message#getPayload()
     * @see Nodes.GossipNode#isLeader()
     * @see Nodes.Raft.RaftNode#handleAppendEntries()
     * @see Nodes.Raft.RaftNode#handleCommitIndex()
     * @see Nodes.Raft.RaftNode#handleHeartbeat()
     * @see Nodes.Raft.RaftNode#handleAppendEntriesReply()
     * @see Nodes.Raft.RaftNode#handleVoteRequest()
     * @see Services.HeartbeatService#processSync()
     * @see Services.HeartbeatService#processCommit()
     * @see Services.HeartbeatService#processDiscoveryRequest()
     */
    @SuppressWarnings("deprecation")
    private void receiveMulticast() { 
        if (isSuspended) {
            try {
                Thread.sleep(1000);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        try (MulticastSocket multicastSocket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(MULTICAST_GROUP);
            multicastSocket.joinGroup(group);
            System.out.println("Listening on multicast group: " + MULTICAST_GROUP);
    
            byte[] buffer = new byte[2048]; 
    
            while (true) {
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                multicastSocket.receive(packet);
    
                // Extract the header (first 4 bytes) and payload
                String header = new String(packet.getData(), 0, 4);
                byte[] payload = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());
    
                if ("COMP".equals(header)) {
                    System.out.println("Received compressed message");
                    // Decompress and deserialize
                    byte[] decompressedData = CompressionUtils.decompress(payload);
                    Message message = (Message) network.deserialize(decompressedData);

                    
                    OPERATION op = message.getOperation();
                    Object obj = message.getPayload();
                    //Document doc = (Document) obj;
                        
                    switch (op) {
                        case APPEND_ENTRIES:
                            if(!this.gossipNode.isLeader()) {
                                System.out.println("[DEBUG]->APPEND ENTRIES RECIEVED");
                                AppendEntriesArgs args = (AppendEntriesArgs) obj;
                                gossipNode.getRaftNode().handleAppendEntries(args, message.getUdpPort());
                            }
                            break;
                        case SYNC: //for syncing purposes
                            if(this.gossipNode.isLeader()){ break;}
                            // System.out.println("IN SYNC BROADCAST RECEIVE");
                            // System.out.print("\t");
                            // System.out.println(obj);
                            processSync(obj);
                            //sendSyncAck(UUID targetNodeId, int target_port)
                            break;
                        case REVERT: //for syncing purposes
                            if(this.gossipNode.isLeader()){ break;}
                            gossipNode.getDocuments().revertChanges();
                            break;
                        case COMMIT_INDEX:
                            if(this.gossipNode.isLeader()){ break;}
                            gossipNode.getRaftNode().handleCommitIndex(message);
                            break;
                        case CONSISTENCY_CHECK:
                            if(this.gossipNode.isLeader()){ break;}
                            gossipNode.getRaftNode().handleConsistencyCheck(message);
                            break;
                        default:
                            System.err.println("This operation is not supported in this part of the code [COMP], BIG BUG1: " + op);
                    }

                } else if ("UNCO".equals(header)) {

                    Message message = (Message) network.deserialize(payload);
                    OPERATION op = message.getOperation();
                    switch (op) {
                        case HEARTBEAT: // reply to hearbeats 
                        //System.out.println("\n\tIM HERE IN CASE HEARTBEAT\n");
                            replyToHeartbeat(message);
                            //gossipNode.getRaftNode().handleHeartbeat(message);

                            break;
                        case LHEARTBEAT: // reply to hearbeats from leader
                            //RaftNode rn = gossipNode.getRaftNode();
                            gossipNode.getRaftNode().handleHeartbeat(message);
                            UUID senderId = message.getNodeId();
                            lastReceivedHeartbeats.put(senderId.toString(), System.currentTimeMillis());
                            break;
                        case APPEND_ENTRIES_REPLY:
                            if(gossipNode.isLeader()) {
                                AppendEntriesReply reply = (AppendEntriesReply) message.getPayload();
                                gossipNode.getRaftNode().handleAppendEntriesReply(reply);
                            }
                            break;
                        case COMMIT: // for commit purposes
                            processCommit();
                            System.out.println("\n\n\t COMMITED -> " + message + "\n\n");
                            
                            break;
                        case DISCOVERY: // for NEW NODE 
                            if (this.gossipNode.isLeader()){
                                processDiscoveryRequest(message);
                            }
                            break;
                        case VOTE_REQ:
                            // System.out.println("inside receive VOTE_REQ: \n");
                            // System.out.println(message);
                            RaftNode rn = gossipNode.getRaftNode();
                            RequestVoteArgs rvargrs = (RequestVoteArgs) message.getPayload();
                            if ( rvargrs.getCandidateId().equals(gossipNode.getNodeId()) ){break;}
                            //System.out.println("\n\tmessage conrtent\n" + message);
                            rn.handleVoteRequest(rvargrs, message.getUdpPort());
                            break;
                        default:
                            System.err.println("This operation is not supported in this part of the code [UNCOMP], BIG BUG2: " + op);
                    }

                    //System.out.println(this.gossipNode.getNodeName()+"Received uncompressed message: " + message);
                } else {
                    System.out.println("Unknown message type: " + header);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Error receiving message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /*
                                                        ██    ██ ███    ██ ██  ██████  █████  ███████ ████████ 
                                                        ██    ██ ████   ██ ██ ██      ██   ██ ██         ██    
                                                        ██    ██ ██ ██  ██ ██ ██      ███████ ███████    ██    
                                                        ██    ██ ██  ██ ██ ██ ██      ██   ██      ██    ██    
                                                        ██████  ██   ████ ██  ██████ ██   ██ ███████    ██    
                                                                                                            
                                                                                                        
     */
    /**
    * Receives unicast messages and processes them based on the operation type.
    * Listens on a UDP socket and handles incoming messages.
    * Messages can be compressed or uncompressed.
    * Compressed messages are decompressed before processing.
    * Supported operations:
    * - HEARTBEAT_ACK: Adds the sender as a known node.
    * - ACK: Processes a sync acknowledgment if the node is the leader.
    * - FULL_SYNC_ACK: Processes a full sync acknowledgment if the node is the leader.
    * - VOTE_ACK: Handles a vote acknowledgment.
    * - VOTE_REQ: Handles a vote request if the node is not the candidate.
    * - FULL_SYNC: Handles a full sync request if the node is the leader.
    * Unsupported operations are logged as errors.
    *
    * @see shared.OPERATION
    * @see utils.CompressionUtils#decompress()
    * @see utils.network#deserialize()
    * @see shared.Message#getOperation()
    * @see shared.Message#getPayload()
    * @see Nodes.GossipNode#isLeader()
    * @see Nodes.GossipNode#startFullSyncProcess()
    * @see Nodes.GossipNode#addFullSyncACK()
    * @see Nodes.Raft.RaftNode#handleAppendEntries()
    * @see Nodes.Raft.RaftNode#addNewNodeToMaps()
    * @see Nodes.Raft.RaftNode#handleVoteResponse()
    * @see Nodes.Raft.RaftNode#handleVoteRequest()
    * @see Nodes.Raft.RaftNode#handleAppendEntriesReply()
    * @see Services.HeartbeatService#leaderRespondToFullSync()
    * @see Services.HeartbeatService#addKnownNode()
    * @see Services.HeartbeatService#processACK()
    */
    private void receiveMessage() {
        if (isSuspended) {
            try {
                Thread.sleep(1000);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        byte[] buffer = new byte[2048]; 
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        
        try{
            //socket.setSoTimeout(5000);
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    socket.receive(packet); 

                    System.out.println("Message received in receiveMessage on port: " + packet.getPort());
                    //System.out.println("Packet content: " + new String(packet.getData(), 0, packet.getLength()));
                    // Extract the header (first 4 bytes) and payload
                    String header = new String(packet.getData(), 0, 4);
                    byte[] payload = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());


                    if ("COMP".equals(header)) {
                        System.out.println("Received compressed message");
                        // Decompress and deserialize
                        byte[] decompressedData = CompressionUtils.decompress(payload);
                        Message message = (Message) network.deserialize(decompressedData);
                        OPERATION op = message.getOperation();
                        Object obj = message.getPayload();

                        System.out.println("PROCESSING COMP UNICAST MESSAGE : " + message);
                        System.out.println("PROCESSING COMP UNICAST OP : " + op);
                        //Document doc = (Document) obj;
                            
                        switch (op) {
                            case FULL_SYNC: //for full sync for new node
                            if (this.gossipNode.isLeader()){
                                // Message msg = Message.FullSyncMessage("FULL_SYNC:" + gossipNode.getNodeId() + ":" + sync_port + ":"  + System.currentTimeMillis());
                                String receivedMsg = (String)obj;
                                // Split string to get usefull data
                                String[] parts = receivedMsg.split(":");
                                String nodeId = parts[1];
                                String port = parts[2];
                                // Get the documents list
                                Message msg = this.gossipNode.startFullSyncProcess(); 
                                // Send the document list to new node
                                leaderRespondToFullSync(msg, UUID.fromString(nodeId), Integer.parseInt(port));}
                                break;
                            case QUEUE_TRANSFER:
                                if (gossipNode.getRaftNode().getNodeState() == NodeState.LEADER) {
                                    System.out.println("Received QUEUE_TRANSFER message");
                                    //gossipNode.getRaftNode(). handleQueueTransfer(message);
                                }
                                break;
                            case APPEND_ENTRIES:
                                if(!this.gossipNode.isLeader()) {
                                    System.out.println("[DEBUG]->FIXING LOG REPLIICATION MISSING -> APPEND ENTRIES RECIEVED");
                                    AppendEntriesArgs args = (AppendEntriesArgs) obj;
                                    gossipNode.getRaftNode().handleAppendEntries(args, message.getUdpPort());
                                }
                                break;
                            default:
                                System.err.println("This operation is not supported in this part of the code, BIG BUG3: " + op);
                        }


                        System.out.println("Received compressed message");
                        
                    } else if ("UNCO".equals(header)) {
                        
                        Message message = (Message) network.deserialize(payload);
                        OPERATION op = message.getOperation();
                        System.out.println("PROCESSING UNCO UNICAST MESSAGE : " + message);
                        System.out.println("PROCESSING UNCO UNICAST OP : " + op);
                        switch (op) {
                            case HEARTBEAT_ACK: // reply to hearbeats 
                                addKnownNode(message);
                                break;
                            
                            case ACK: // sync acks
                                if (this.gossipNode.isLeader()){
                                    System.out.println("\n\n\t ACK RECEIVED FOR OPERATION: " + message);
                                    Object obj = message.getPayload();
                                    processACK(obj);
                                    
                                }
                                break;
                            case FULL_SYNC_ACK: 
                                if (this.gossipNode.isLeader()){
                                    
                                    //FULL SYNC ACK RECEIVED-> Message{header='FULL_SYNC_ACK', payload=[OPERATION]
                                    //:9ab092c14c916570643ac53d392ac21ae7db31cd671872c474c5d09380fd450f, 
                                    //nodeName='Node-66', nodeId=406cc4eb-d340-4ada-be45-c6f1b219964b, udpPort=10627}
                                    if (gossipNode.isRaftNode()){
                                        System.out.println("\n\n\t FULL-SYNC-ACK RECEIVED [message]: " + message);
                                        RaftNode rn = gossipNode.getRaftNode();
                                        //System.out.println("\n\n\t FULL-SYNC-ACK RECEIVED logs: " + rn.getLogsAsString());
                                        //payload=[index]:6, nodeName='Node-66', nodeId=e85abe9a-2862-41a6-a905-166ca8daaed9, udpPort=10157}
                                        Object obj = message.getPayload();
                                        String parts[] = ((String) obj).split(":");
                                        int index = Integer.parseInt(parts[1]);
                                        System.out.println("ADDING NEW NODE TO MAPS");
                                        rn.addNewNodeToMaps(message.getNodeId(), index);
                                    }
                                    else {
                                        System.out.println("\n\n\t FULL-SYNC-ACK RECEIVED FOR OPERATION: " + message);
                                        Object obj = message.getPayload();
                                        System.out.println("FULL SYNC ACK RECEIVED-> "+ message.toString());
                                        String content = (String) obj;
                                        String[] parts = content.split(":");
                                        String opID = parts[1];
                                        gossipNode.addFullSyncACK(opID);
                                    }
                                }
                                break;
                            case VOTE_ACK:
                                System.out.println("inside receive VOTE_ACK: \n");
                                System.out.println( "REQUESTVOTEREPLY" + ((RequestVoteReply) message.getPayload()).toString() );
                                RequestVoteReply rvreply = (RequestVoteReply) message.getPayload();
                                RaftNode rn = gossipNode.getRaftNode();
                                rn.handleVoteResponse(rvreply);
                                
                                break;
                            case VOTE_REQ:
                                // System.out.println("inside receive VOTE_REQ: \n");
                                // System.out.println(message);
                                rn = gossipNode.getRaftNode();
                                RequestVoteArgs rvargrs = (RequestVoteArgs) message.getPayload();
                                if ( rvargrs.getCandidateId().equals(gossipNode.getNodeId()) ){break;}
                                rn.handleVoteRequest(rvargrs, message.getUdpPort());
                                break;
                            case APPEND_ENTRIES_REPLY:
                                System.out.println("PROCESSING APPEND ENTRIES REPLYS : " + gossipNode.isLeader());
                                if(gossipNode.isLeader()) {
                                    AppendEntriesReply reply = (AppendEntriesReply) message.getPayload();
                                    gossipNode.getRaftNode().handleAppendEntriesReply(reply);
                                }
                                break;
                                
                            default:
                                System.err.println("This operation is not supported in this part of the code, BIG BUG4: " + op);
                        }
                        //System.out.println(this.gossipNode.getNodeName()+"Received uncompressed message: " + message);
                    } else {
                        System.out.println("Unknown message type: " + header);
                    }
                } catch (SocketTimeoutException e) {
                    // Timeout occurred; continue waiting for messages
                    System.out.println("Socket timeout; no message received.");
                }
            }
        }
         catch (IOException | ClassNotFoundException e) {
            System.err.println("Error receiving message: " + e.getMessage());
            System.out.println("NODE: " + gossipNode.getNodeName() + "- STATE: " + gossipNode.getRaftNode().getNodeState());
            e.printStackTrace();
        }
        catch (Exception e) {
            if (gossipNode.isRunning()) {
                System.err.println("Error receiving message: " + e.getMessage());
            }
        }
    }

    

/*
███████ ██    ██ ███    ██        █████   ██████ ██   ██ 
██       ██  ██  ████   ██       ██   ██ ██      ██  ██  
███████   ████   ██ ██  ██ █████ ███████ ██      █████   
     ██    ██    ██  ██ ██       ██   ██ ██      ██  ██  
███████    ██    ██   ████       ██   ██  ██████ ██   ██ 
                                                                                    
 */
    /**
    * Processes a batch of operations received from a sync request.
    * @param batch The batch of operations to process.
    * @return The operation ID and leader info if processing is successful, null otherwise.
    * @see Resources.Document#fromString()
    * @see shared.Message#getOperation()
    * @see Nodes.GossipNode#processOP()
    */
    public String processBatch(String batch) {
        this.gossipNode.getDocuments().createTempMap();
        //tempListTimestamp = System.currentTimeMillis();

        System.out.println("\n\n\t\tINSIDE PROCESS BATCH\n\n\n");
        // Split the batch into operation ID and operations
        String[] parts = batch.split(";", 3); // split the message into the each section of it
        String operationId = parts[0];
        String leaderInfo = parts[1];
        String operations = parts[2];
        System.out.println("\t\t" + leaderInfo + "\n\t" + operations );
        System.out.println("Processing batch with Operation ID: " + operationId);
        System.out.println("\nThe leadrInfo: " + leaderInfo);
        // Split individual operations
        String[] operationArray = operations.split("\\$");
        //boolean result = false;

        try{
            for (String operation : operationArray) {
                System.out.println("\nProcessing operation for SYNC: " + operation);
                String[] _op = operation.split(";");
                String op = _op[0];
                String _doc = _op[1];
                Document doc = Document.fromString(_doc);
                gossipNode.processOP(op, doc);
            }
            System.out.println("End of Sync: " + gossipNode.getDocuments().getDocuments().toString());

            return (operationId +":" +leaderInfo );
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        
    }

        /*
                                    ██████  ██    ██ ███████ ██████  ██       ██████   █████  ██████  ██ ███    ██  ██████      
                                    ██    ██ ██    ██ ██      ██   ██ ██      ██    ██ ██   ██ ██   ██ ██ ████   ██ ██           
                                    ██    ██ ██    ██ █████   ██████  ██      ██    ██ ███████ ██   ██ ██ ██ ██  ██ ██   ███     
                                    ██    ██  ██  ██  ██      ██   ██ ██      ██    ██ ██   ██ ██   ██ ██ ██  ██ ██ ██    ██     
                                    ██████    ████   ███████ ██   ██ ███████  ██████  ██   ██ ██████  ██ ██   ████  ██████      
                                                                                                                                
                                                                                       
     */    



    /**
    * Processes a sync request received from another node.
    * @param obj The sync request payload.
    * @return true if the sync is processed successfully, false otherwise.
    * @see Nodes.GossipNode#getDocuments()
    * @see Nodes.DocumentsDB#lock()
    * @see Services.HeartbeatService#processBatch()
    * @see Services.HeartbeatService#sendSyncAck()
    */
    private boolean processSync(Object obj){
        gossipNode.getDocuments().lock();
        String infoLeader = processBatch((String)obj);
        System.out.println(infoLeader);
        if(infoLeader != null){
            String[] parts = infoLeader.split(":");
            String opID = parts[0]; //OP id
            String LID = parts[1]; //Leader id
            String port = parts[2]; //Leader port
            sendSyncAck(UUID.fromString(LID), Integer.parseInt(port), opID);
        }
        return false;
    }

    // private void sendSyncAckTCPServer(Message msg) {
    //     try (
    //         Socket tcpS = new Socket("localhost", TCP_PORT);
    //         ObjectOutputStream out = new ObjectOutputStream(tcpS.getOutputStream());
    //         ObjectInputStream in = new ObjectInputStream(tcpS.getInputStream())
    //     ) {
    //         // Send the custom Message object
    //         out.writeObject(msg);
    //         // Receive the response from the server
    //         Message response = (Message) in.readObject();
    //         System.out.println("Server Response: " + response);

    //     } catch (IOException | ClassNotFoundException e) {
    //         e.printStackTrace();
    //     }
    // }
    /**
    * Sends a sync acknowledgment to the target node.
    * @param targetNodeId The UUID of the target node.
    * @param target_port The port of the target node.
    * @param operationID The ID of the operation being acknowledged.
    * @see shared.Message#replySyncMessage()
    * @see Services.HeartbeatService#sendUncompMessage()
    */
    @SuppressWarnings("deprecation")
    private void sendSyncAck(UUID targetNodeId, int target_port, String operationID) {
        Message msg = Message.replySyncMessage("ACK:" + gossipNode.getNodeId() + ":" + operationID + ":" + 
                                                    heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement() + ":" + System.currentTimeMillis());
        sendUncompMessage(msg, targetNodeId, target_port);

        System.out.println("ACK PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
    }
    @SuppressWarnings("deprecation")
    public void sendSyncAck(UUID targetNodeId, int target_port) {
        Message msg = Message.replySyncMessage("ACK:" + gossipNode.getNodeId() +":" + 
                                                    heartbeatCounters.get(gossipNode.getNodeId()).getAndIncrement() + ":" + System.currentTimeMillis());
        sendUncompMessage(msg, targetNodeId, target_port);

        System.out.println("ACK PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
    }
    /**
    * Processes a sync acknowledgment received from another node.
    * @param obj The acknowledgment payload.
    */
    private void processACK(Object obj){
        String content = (String) obj;
        String[] parts = content.split(":");
        String senderNodeId = parts[1];
        String opID = parts[2];
        
        this.gossipNode.addACK(UUID.fromString(senderNodeId), opID);
    }

                                                            /*
         ██████  ██████  ███    ███ ███    ███ ██ ████████ 
        ██      ██    ██ ████  ████ ████  ████ ██    ██    
        ██      ██    ██ ██ ████ ██ ██ ████ ██ ██    ██    
        ██      ██    ██ ██  ██  ██ ██  ██  ██ ██    ██    
         ██████  ██████  ██      ██ ██      ██ ██    ██    
    */
    /**
    * Processes a commit operation.
    * Unlocks the lock.
    */
    private void processCommit(){
        gossipNode.getDocuments().unlock();;
    }




















    
    

    /*
    
██████  ██ ███████  ██████  ██████  ██    ██ ███████ ██████  ██    ██ 
██   ██ ██ ██      ██      ██    ██ ██    ██ ██      ██   ██  ██  ██  
██   ██ ██ ███████ ██      ██    ██ ██    ██ █████   ██████    ████   
██   ██ ██      ██ ██      ██    ██  ██  ██  ██      ██   ██    ██    
██████  ██ ███████  ██████  ██████    ████   ███████ ██   ██    ██    
                                                                                                                                                                                                                                                                               
    */
    /**
    * Processes a discovery request received from a new node.
    * Leader Node will send the ACK for the request.
    * @param message The discovery request message.
    * @see Services.HeartbeatService#sendACKDiscovery()
    */
    private void processDiscoveryRequest(Message message){
        Object obj = message.getPayload();
        String content = (String) obj;
        System.out.println("\tCotent inside add to knownNodes->  " + content+"\n\n");
        String[] parts = content.split(":"); //String PAYLOAD = "WHOS_THE_LEADER:" + nodeId + ":" + port + ":" + System.currentTimeMillis();
        String senderNodeId = parts[1];
        int port = Integer.parseInt(parts[2]);

        sendACKDiscovery(UUID.fromString(senderNodeId),port );
        gossipNode.addKnownNode(UUID.fromString(senderNodeId), port);
    }
    /**
    * Sends a discovery acknowledgment to the target node.
    * @param targetNodeId The UUID of the target node.
    * @param target_port The port of the target node.
    * @see shared.Message#replyDiscoveryMessage()
    * @see Services.HeartbeatService#sendUncompMessage()
    */
    @SuppressWarnings("deprecation")
    private void sendACKDiscovery(UUID targetNodeId, int target_port) {
        Message msg = Message.replyDiscoveryMessage("DISCOVERY_ACK:" + gossipNode.getNodeId() + ":" + this.udpPort + ":" + System.currentTimeMillis());
        sendUncompMessage(msg, targetNodeId, target_port );

        System.out.println("DISCOVERY_ACK PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " to port " + target_port);
    }
    /**
    * Broadcasts a discovery request to find the leader node.
    * @param port The port to use for the sync socket.
    * @param msg The discovery request message.
    * @return The response from the leader node, or null if no response is received.
    * @see utils.network#deserialize()
    * @see Services.HeartbeatService#handleFullSyncResponse()
    */
    private String broadcastDiscovery(int port, Message msg){
        try(DatagramSocket syncSocket = new DatagramSocket(port)){
            syncSocket.setSoTimeout(5000);
            //2nd sent the multicast
            broadcast(msg, false);
            System.out.println("Waiting for response to discovery message...");

            byte[] buffer = new byte[2048]; 
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                // Wait for a response
                syncSocket.receive(packet);
    
                // Deserialize the response
                byte[] data = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());
                Message responseMessage = (Message) network.deserialize(data);
    
                // Process the response
                if (responseMessage.getOperation() == OPERATION.DISCOVERY_ACK) {
                    System.out.println("Received sync response: " + responseMessage);
                    // Process sync response (e.g., synchronize documents or state)
                    String res=  handleFullSyncResponse(responseMessage);
                    return res;
                } else {
                    System.err.println("Unexpected operation: " + responseMessage.getOperation());
                }
            } catch (SocketTimeoutException e) {
                System.err.println("Timeout waiting for sync response.");
            } catch (ClassNotFoundException e) {
                            e.printStackTrace();
            }
            
        }catch(IOException e ){
            e.printStackTrace();
        }
        return null;
    }

    /*
███████ ██    ██ ██      ██          ███████ ██    ██ ███    ██  ██████ 
██      ██    ██ ██      ██          ██       ██  ██  ████   ██ ██      
█████   ██    ██ ██      ██          ███████   ████   ██ ██  ██ ██      
██      ██    ██ ██      ██               ██    ██    ██  ██ ██ ██      
██       ██████  ███████ ███████     ███████    ██    ██   ████  ██████ 

     */

     /**
     * Handles the response to a full sync request.
     * @param response The full sync response message.
     * @return The leader node ID and port.
     */
    private String handleFullSyncResponse(Message response){
        Object obj = response.getPayload();
        String content = (String) obj;
        //System.out.println("\tContent inside add to knownNodes->  " + content+"\n\n");
        String[] parts = content.split(":"); //("DISCOVERY_ACK:" + gossipNode.getNodeId() + ":" + this.udpPort + ":" + System.currentTimeMillis());
        String LeaderNodeId = parts[1];
        String Leader_UDP_port = parts[2];
        int port = Integer.parseInt(Leader_UDP_port);
        gossipNode.addKnownNode(UUID.fromString(LeaderNodeId),port );

        return LeaderNodeId + ":" + Leader_UDP_port;        
    }
    /**
    * Sends a full sync request to the leader node.
    * @param leaderNodeId The UUID of the leader node.
    * @param leader_port The port of the leader node.
    * @param sync_port The port to use for the sync socket.
    * @see shared.Message#FullSyncMessage()
    * @see Services.HeartbeatService#sendCompMessage()
    */
    @SuppressWarnings("deprecation")
    private void fullSyncRequest(UUID leaderNodeId, int leader_port, int sync_port) {
        Message msg = Message.FullSyncMessage("FULL_SYNC:" + gossipNode.getNodeId() + ":" + sync_port + ":"  + System.currentTimeMillis());
        sendCompMessage(msg, leaderNodeId, leader_port);
        System.out.println("FULL_SYNC SENT FROM " + gossipNode.getNodeId() + " to " + leader_port);
    }
    /**
     * Initiates a full sync process with the leader node.
     * 
     * This method is called by a new node joining the cluster to synchronize its state with the leader.
     * It performs the following steps:
     * 1. Creates a new DatagramSocket with the specified sync port.
     * 2. Sets a timeout of 50 seconds on the socket to wait for a response from the leader.
     * 3. Sends a full sync request to the leader node using the `fullSyncRequest` method.
     * 4. Waits for the updated data from the leader using the `waitFullSync` method.
     * 5. Processes the received document list using the `processDocumentList` method.
     * 6. Sends a reply to the leader acknowledging the completion of the full sync process.
     * 
     * If any exceptions occur during the process, they are caught and logged.
     * 
     * @param leaderID The UUID of the leader node.
     * @param leader_port The port number of the leader node.
     * @param sync_port The port number to use for the sync socket.
     * @see shared.Message#replyFullSyncMessage()
     * @see Services.HeartbeatService#fullSyncRequest()
     * @see Services.HeartbeatService#waitFullSync()
     * @see Services.HeartbeatService#processDocumentList()
     * @see Services.HeartbeatService#sendUncompMessage()
     */
    @SuppressWarnings("deprecation")
    private void fullSyncInnit(UUID leaderID, int leader_port, int sync_port ){
        try(DatagramSocket syncSocket = new DatagramSocket(sync_port)){
            syncSocket.setSoTimeout(50000);
            //2nd sent the request
            fullSyncRequest(leaderID, leader_port, sync_port);
            // Await for updated DB
            Message data = waitFullSync(syncSocket);
            System.out.println(data);
            Object obj = data.getPayload();

            System.out.println("\n\n\n\t\t\tFULL SYNC RECEIVED\n\n\n");

            System.out.println("OBJECT: " + obj);
            String list = (String) obj; 
            String r = processDocumentList(list);
            System.out.println("Processed document list full sync result: ->" + r);
            //Processed document list full sync result: ->c594c62794ecd0e89ab09de38cc5576cce273b7cdc3778e6cfe95abf78f3c74c:f107eb3c-5d00-46f4-82a9-700a94108701:9723
            String parts[] = r.split(":");

            String op = parts[0];
            // String nodeID = parts[1];
            // String port = parts[2];
            
            Message replyMessage = Message.replyFullSyncMessage("[OPERATION]:"+op);
            sendUncompMessage(replyMessage, leaderID, leader_port);
            
        }catch(SocketException e){
            System.out.println("SOCKET EXCEPTION");
            e.printStackTrace();
        }
    }
    /**
     * Waits for the full sync data from the leader node.
     * 
     * This method is called by the `fullSyncInnit` method to wait for the updated data from the leader.
     * It performs the following steps:
     * 1. Creates a byte array to store the received data.
     * 2. Creates a DatagramPacket to receive the data.
     * 3. Initializes a counter for the number of attempts to receive the data.
     * 4. Enters a loop that continues until the correct data is received or the maximum number of attempts is reached.
     *    - Receives a packet using the `receive` method of the DatagramSocket.
     *    - Extracts the header and payload from the received packet.
     *    - If the header indicates a compressed message, decompresses the payload using the `CompressionUtils.decompress` method.
     *    - Deserializes the decompressed data into a Message object.
     *    - Checks the operation type of the received message.
     *    - If the operation type is `FULL_SYNC_ANS`, returns the received message.
     *    - If the operation type is not recognized, logs an error and decrements the attempt counter.
     * 5. If the maximum number of attempts is reached without receiving the correct data, returns null.
     * 
     * If any exceptions occur during the process, they are caught and logged.
     * 
     * @param socket The DatagramSocket to use for receiving the data.
     * @return The received Message object containing the full sync data, or null if the data is not received.
     * @see utils.CompressionUtils#decompress()
     * @see utils.network#deserialize()
     */
    @SuppressWarnings("unused")
    private Message waitFullSync(DatagramSocket socket){
        byte[] buf = new byte[2048];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        int attempts = 100;
        try{
            while (attempts > 0) {
                socket.receive(packet);
                //Extract header's first 4 bytes
                String header = new String(packet.getData(), 0, 4);
                // Get compressed list of new docs
                byte[] payload = Arrays.copyOfRange(packet.getData(), 4, packet.getLength());
                
                byte[] decompressedData = CompressionUtils.decompress(payload);
                Message msg = (Message)network.deserialize(decompressedData);
                System.out.println("MESSAGE: " + msg);
                OPERATION op = msg.getOperation();

                if(op == OPERATION.FULL_SYNC_ANS){
                    return msg;
                }
                System.err.println("Operation not premited");
                attempts--;
            }
        }catch(Exception e){
            System.out.println("Error in waitFullSYnc: " + e);
        }

        return null;
    }
    /**
     * Sends a full sync response to the target node.
     *
     * @param msg The full sync message to send.
     * @param targetNodeId The UUID of the target node.
     * @param target_port The port of the target node.
     * @see Services.HeartbeatService#sendCompMessage()
     */
    private void leaderRespondToFullSync(Message msg, UUID targetNodeId, int target_port) {
        sendCompMessage(msg, targetNodeId, target_port);
        System.out.println("FULL_SYNC PACKET SENT FROM " + gossipNode.getNodeId() + " to " + targetNodeId + " with counter " + getHeartbeatCounter());
        System.out.println(msg);
    }
    /**
     * Processes a list of documents received from a full sync operation.
     *
     * @param batch The batch of documents to process.
     * @return The operation ID and leader info if processing is successful, null otherwise.
     * @see Nodes.DocumentsDB#updateOrAddDocument()
     */
    public String processDocumentList(String batch) {
        System.out.println("\n\n\t\tINSIDE PROCESS FULL SYNC\n\n\n");
        System.out.println("\n\n\t\t"+batch+"\n\n\n");
        // Split the batch into operation ID and operations
        String[] parts = batch.split(";", 3); // split the message into the each section of it
        if (parts.length < 3) {
            System.err.println("Invalid batch format: " + batch);
            return null;
        }
        String operationId = parts[0];
        String leaderInfo = parts[1];
        String documents = parts[2];
        System.out.println("\t\t" + leaderInfo + "\n\t" +" documents after splitting batch" +documents );
        System.out.println("Processing batch with Operation ID: " + operationId);
        
        // Split individual operations
        String[] docs = documents.split("\\$"); // Split individual operations on literal "$" instead of using the regex meaning of $
       
        System.out.println("Inside documents list after splitting: " + docs);

        try{
            for (String _doc : docs) {
                System.out.println("\nProcessing documents list: " + _doc);
                Document doc = Document.fromString(_doc);
                gossipNode.getDocuments().updateOrAddDocument(doc);
            }
            return (operationId +":" +leaderInfo );
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
        
    }

    /**
     * Initiates a full sync process for a new node joining the cluster.
     * @see Services.HeartbeatService#fullSyncInnit()
     */
    @SuppressWarnings("deprecation")
    public void syncNewElement(){
        // 1st prepare the message for multicast discovery
        int port_for_syncing = 9999;
        Message discoveryMessage = Message.discoveryMessage(this.gossipNode.getNodeId(), port_for_syncing);
        //Function to send multicast asking who is the leader
        String LeaderACK = broadcastDiscovery(port_for_syncing, discoveryMessage);

        String[] parts = LeaderACK.split(":");
        String leaderID = parts[0];
        String leader_port = parts[1];
        
        fullSyncInnit(UUID.fromString(leaderID), Integer.parseInt(leader_port), port_for_syncing );
    }


/*
██████   █████  ███████ ████████     ███    ███ ███████ ████████ ██   ██  ██████  ██████  ███████ 
██   ██ ██   ██ ██         ██        ████  ████ ██         ██    ██   ██ ██    ██ ██   ██ ██      
██████  ███████ █████      ██        ██ ████ ██ █████      ██    ███████ ██    ██ ██   ██ ███████ 
██   ██ ██   ██ ██         ██        ██  ██  ██ ██         ██    ██   ██ ██    ██ ██   ██      ██ 
██   ██ ██   ██ ██         ██        ██      ██ ███████    ██    ██   ██  ██████  ██████  ███████
 */
    /**
     * Initiates a full sync process for a new node joining the cluster. - For RAFT Protocol
     * @see shared.Message#discoveryMessage()
     * @see Services.HeartbeatService#broadcastDiscovery()
     * @see Services.HeartbeatService#initializeRaftState()
     */
    @SuppressWarnings("deprecation")
    public void syncNewElementRaftCluster(){
        // 1st prepare the message for multicast discovery
        int port_for_syncing = 9999;
        Message discoveryMessage = Message.discoveryMessage(this.gossipNode.getNodeId(), port_for_syncing);
        //Function to send multicast asking who is the leader
        String LeaderACK = broadcastDiscovery(port_for_syncing, discoveryMessage);

        String[] parts = LeaderACK.split(":");
        String leaderID = parts[0];
        String leader_port = parts[1];
        
        initializeRaftState(UUID.fromString(leaderID), Integer.parseInt(leader_port), port_for_syncing);
    }

    private int getAvailablePort() {
        for (int port = MIN_PORT; port <= MAX_PORT; port++) {
            if (!usedPorts.contains(port)) {
                try (DatagramSocket testSocket = new DatagramSocket(port)) {
                    usedPorts.add(port);
                    return port;
                } catch (IOException e) {
                    // Port is not available, continue to next
                }
            }
        }
        throw new RuntimeException("No available ports found");
    }

    /**
     * Initializes the Raft state of the node by requesting a full state sync from the leader.
     *
     * @param leaderID   The UUID of the leader node.
     * @param leader_port The port number of the leader node.
     * @param sync_port  The port number to use for the sync socket.
     * 
     * @see Services.HeartbeatService#fullSyncRequest()
     * @see Services.HeartbeatService#waitFullSync()
     * @see Services.HeartbeatService#sendUncompMessage()
     * @see shared.Message#replyFullSyncMessage()
     * @see Nodes.Raft.RaftNode#handleSyncRequest()
     * @see Nodes.Raft.RaftNode#stepDown()
     */
    public void initializeRaftState(UUID leaderID, int leader_port, int sync_port) {
        int actualSyncPort = getAvailablePort();
        try (DatagramSocket syncSocket = new DatagramSocket(actualSyncPort)) {
            syncSocket.setSoTimeout(50000);
            
            // Request full state from leader
            fullSyncRequest(leaderID, leader_port, actualSyncPort);
            Message response = waitFullSync(syncSocket);
            
            if (response != null) {
                String payload = (String) response.getPayload();
                System.out.println("PAYLOAD INITIALIZE RAFT STATE : " + payload);
                RaftNode raftNode = gossipNode.getRaftNode();
                int index = raftNode.handleSyncRequest(payload) ;
                String[] sections = payload.split(";");
                
                // // Update term
                int leaderTerm = Integer.parseInt(sections[2]);
                
                // Send acknowledgment
                Message ackMsg = Message.replyFullSyncMessage("[index]:" + index, this.gossipNode.getNodeName(), 
                                            this.gossipNode.getNodeId(), this.getUDPport());
                
                sendUncompMessage(ackMsg, leaderID, leader_port);
                
                // Start as follower
                raftNode.stepDown(leaderTerm);
            }
        } catch (SocketException e) {
            System.err.println("Error during Raft state initialization: " + e.getMessage());
            e.printStackTrace();
        }
    }

    




    /**
     * Detects node failures based on the last received heartbeat timestamps.
     * If a node's last heartbeat is older than the failure timeout, it is considered failed and therefore removed from the knownNodes map.
     * 
     * @see Nodes.GossipNode#removeKnownNode()
     * @see Nodes.GossipNode#isRaftNode()
     * @see Nodes.Raft.RaftNode#handleLeaderFailure()
     */
    private void detectFailures() {
        if (isSuspended) {
            try {
                Thread.sleep(1000);
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        long currentTime = System.currentTimeMillis();
        System.out.println(getName());
        //System.out.println("detecting failures");
        // First, handle general node failures
        // gets all the sets in the map of nodes timestamps
        for (Map.Entry<String, Long> entry : lastReceivedHeartbeats.entrySet()) {
            String nodeId = entry.getKey();
            long lastReceivedTime = entry.getValue();

            if (currentTime - lastReceivedTime > FAILURE_TIMEOUT) {
                System.out.println("Node " + nodeId + " is considered failed.");
                gossipNode.removeKnownNode(UUID.fromString(nodeId));

                // If this is a Raft node, handle Raft-specific failure detection
                if (gossipNode.isRaftNode()) {
                    // If the failed node was the leader
                    RaftNode raftNode = gossipNode.getRaftNode();
                    if (raftNode.getLeaderId() != null && 
                        raftNode.getLeaderId().toString().equals(nodeId)) {
                        
                        System.out.println("[DEBUG] Failed node was the leader. Starting election process.");
                        raftNode.handleLeaderFailure();
                    }
                }
            }
        }
    }
    /**
     * Interrupts the heartbeat service thread and shuts down the scheduler and socket.
     */
    @Override
    public void interrupt() {
        super.interrupt();
        scheduler.shutdown();
        socket.close();
    }
    /**
     * Returns the map of heartbeat counters for each node.
     *
     * @return The map of heartbeat counters.
     */
    public Map<UUID, AtomicInteger> getHeartbeatCounters() {
        return heartbeatCounters;
    }
    /**
     * Returns the map of last received heartbeat timestamps for each node.
     *
     * @return The map of last received heartbeat timestamps.
     */
    public Map<String, Long> getLastReceivedHeartbeats() {
        return lastReceivedHeartbeats;
    }






    
    /*
  _   _ ______ _________          ______  _____  _  _______ _   _  _____ 
 | \ | |  ____|__   __\ \        / / __ \|  __ \| |/ /_   _| \ | |/ ____|
 |  \| | |__     | |   \ \  /\  / / |  | | |__) | ' /  | | |  \| | |  __ 
 | . ` |  __|    | |    \ \/  \/ /| |  | |  _  /|  <   | | | . ` | | |_ |
 | |\  | |____   | |     \  /\  / | |__| | | \ \| . \ _| |_| |\  | |__| |
 |_| \_|______|  |_|      \/  \/   \____/|_|  \_\_|\_\_____|_| \_|\_____|
                                                                         
                                                                         

    */
    /**
     * Sends an uncompressed message to the target node.
     *
     * @param msg The message to send.
     * @param targetNodeId The UUID of the target node.
     * @param target_port The port of the target node.
     * @see utils.network#serialize()
     * @see utils.network#addHeader()
     */
    public void sendUncompMessage(Message msg, UUID targetNodeId, int target_port){
        try{
            byte[] serializedData = network.serialize(msg);
            byte[] finalData = network.addHeader("UNCO", serializedData);
                    
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, target_port);
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Sends a compressed message to the target node.
     *
     * @param msg The message to send.
     * @param targetNodeId The UUID of the target node.
     * @param target_port The port of the target node.
     * @see utils.network#serialize()
     * @see utils.network#addHeader()
     * @see utils.CompressionUtils#compress()
     */
    public void sendCompMessage(Message msg, UUID targetNodeId, int target_port){
        try{
            byte[] serializedData = network.serialize(msg);
            byte[] compressedData = CompressionUtils.compress(serializedData);
            byte[] finalData = network.addHeader("COMP", compressedData);
            
                    
            InetAddress targetAddress = InetAddress.getByName(getNodeIPAddress(targetNodeId));

            DatagramPacket packet = new DatagramPacket(finalData, finalData.length, targetAddress, target_port);
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    

    // get the IP of a node
    private String getNodeIPAddress(UUID nodeId) {
        //static beacuse running on local machine
        return "localhost";
    }

    public void shutdown() {
        gossipNode.getRaftNode().setRunning(false);
        
        // Shutdown scheduler
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler.shutdownNow();
        }
        
        // Close socket
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
    
}

