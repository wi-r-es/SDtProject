package Nodes;
import Services.HeartbeatService;
import shared.Message;
import shared.OPERATION;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.swing.text.DocumentFilter;

import Resources.Document;
public class GossipNode extends Thread {
    private final Node node;
    private final HeartbeatService heartbeatService;


    public GossipNode(Node node) {
        this.node = node;
        this.heartbeatService = new HeartbeatService(this);
    }
    @Override
    public void run() {
        heartbeatService.start();
    }

    public UUID getNodeId() {
        return node.getNodeId();
    }
    public String getNodeName(){
        return node.getNodeName();
    }
    public synchronized boolean addDocument(Document doc){
        return node.addDocument(doc);
    }
    public synchronized int searchDocument(Document doc){
        return node.findDocumentIndex(doc.getId());
    }
    public synchronized boolean updateDocument(int index, Document doc){
        return node.updateDocument(index, doc);
    }
    public synchronized boolean updateDocument(Document doc){
        return node.updateDocument(doc);
    }
    public synchronized boolean removeDocument(Document document){
        return node.removeDocument(document);
    }

    public HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }

    public  List<Map.Entry<UUID, Integer>> getRandomNodes() {
        return node.getRandomNodes();
    }
    public boolean isLeader(){
        return node.isLeader();
    }
    public void addKnownNode(UUID nodeId, int port){
        node.addKnownNode(nodeId, port);
    }
    public void addACK(UUID nodeId, String syncOP){
        node.addACK(nodeId, syncOP);
    }

    // Create updated DB in new Node
    public Message startFullSyncProcess(){
       return node.startFullSyncProcess();
    }

}
