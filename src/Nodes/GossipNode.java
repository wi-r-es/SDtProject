package Nodes;
import Services.HeartbeatService;
import shared.Message;
import shared.OPERATION;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import Nodes.Raft.RaftNode;
import Resources.Document;
/**
 * The GossipNode class represents a node in a gossip-based distributed system. -- better said was supposed too...
 * It extends the Thread class to run as a separate thread.
 */
public class GossipNode extends Thread {
    private final Node node;
    private final HeartbeatService heartbeatService;

    /**
     * Constructor for the GossipNode class.
     *
     * @param node The Node instance associated with this GossipNode.
     */
    public GossipNode(Node node) {
        this.node = (Node) node;
        this.heartbeatService = new HeartbeatService(this);
    }
    /**
     * The run method is executed when the thread starts.
     * It starts the heartbeat service.
     */
    @Override
    public void run() {
        heartbeatService.start();
    }
    // Most of the following functions are basically pipes to be able to call Node methods from HeartBeatService
    // Getters
    public UUID getNodeId() {
        return node.getNodeId();
    }
    public String getNodeName(){
        return node.getNodeName();
    }
    /**
     * Returns the RaftNode instance associated with this GossipNode.
     *
     * @return The RaftNode instance.
     */
    public RaftNode getRaftNode() {
        return (RaftNode) node;
    }
    /**
     * Returns the HeartbeatService instance associated with this GossipNode.
     *
     * @return The HeartbeatService instance.
     */
    public HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }
    public  List<Map.Entry<UUID, Integer>> getRandomNodes() {
        return node.getRandomNodes();
    }
    public boolean isLeader(){
        return node.isLeader();
    }
    /** 
     * Add known node to map (uuid - udp port number).
     */
    public void addKnownNode(UUID nodeId, int port){
        node.addKnownNode(nodeId, port);
    }
     /** 
     * Remove known node from map (uuid - udp port number).
     */
    public void removeKnownNode(UUID nodeId){
        node.removeKnownNode(nodeId);
    }
    /**
     * Add known node to map (uuid - node name).
     */
    public void addKnownNode(UUID nodeId, String name){
        node.addKnownNode(nodeId,  name);
    }
    /**
     * Add ACKS for sync process.
     * 
     * @param nodeId ID of Node that sent the ACK.
     * @param syncOP Operation Id.
    */
    public void addACK(UUID nodeId, String syncOP){
        node.addACK(nodeId, syncOP);
    }
    /**
     * Commmits a full sync operation.
     *
     * @param opID The operation ID of the full sync operation.
     */
    public void addFullSyncACK(String opID){
        node.commitFullSync(opID);
    }

    /**
     * Starts the full sync process to create an updated DB in a new node.
     *
     * @return The message containing the full sync content.
     */
    public Message startFullSyncProcess(){
       return node.startFullSyncProcess();
    }


        /*
                                    ██████  ██    ██ ███████ ██████  ██       ██████   █████  ██████  ██ ███    ██  ██████      
                                    ██    ██ ██    ██ ██      ██   ██ ██      ██    ██ ██   ██ ██   ██ ██ ████   ██ ██           
                                    ██    ██ ██    ██ █████   ██████  ██      ██    ██ ███████ ██   ██ ██ ██ ██  ██ ██   ███     
                                    ██    ██  ██  ██  ██      ██   ██ ██      ██    ██ ██   ██ ██   ██ ██ ██  ██ ██ ██    ██     
                                    ██████    ████   ███████ ██   ██ ███████  ██████  ██   ██ ██████  ██ ██   ████  ██████      
                                                                                                                                
                                                                                       
     */
    /**
     * Processes an operation on a document.
     *
     * @param op       The operation to perform.
     * @param document The document to process.
     */
    public synchronized void processOP(OPERATION op, Document document){
        node.processOP(op, document);
     }
     /**
     * Processes an operation on a document.
     *
     * @param op       The operation to perform (as a string).
     * @param document The document to process.
     */
    public synchronized void processOP(String op, Document document){
        node.processOP(op, document);
     }
     /**
     * Returns the DocumentsDB instance associated with the node.
     *
     * @return The DocumentsDB instance.
     */
    public DocumentsDB getDocuments(){
        return node.getDocuments();
     }

}
