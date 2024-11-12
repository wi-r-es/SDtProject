package Nodes;
import Services.HeartbeatService;
import remote.DocumentManagementInterface;

import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import Documents.Document;

public class GossipNode extends Thread {
    private final Node node;
   // private final List<Node> networkNodes;  // List of other nodes in the network for gossip
    private final HeartbeatService heartbeatService;


    public GossipNode(Node node) {
        this.node = node;
        //this.networkNodes = networkNodes;
        this.heartbeatService = new HeartbeatService(this);

        // Start HeartbeatService as a thread
        //heartbeatService.start();
    }
    @Override
    public void run() {
        heartbeatService.start();
    }

    public String getNodeId() {
        return node.getNodeId();
    }

    public HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }

    public List<String> getRandomNodes() {
        return node.getRandomNodes();
    }
    // public List<Node> getRandomNodes() {
    //     List<Node> targets = new ArrayList<>(networkNodes);
    //     Collections.shuffle(targets);
    //     return targets.subList(0, Math.min(3, targets.size()));  // Gossip to 3 random nodes
    // }





    // Gossip document to a subset of nodes
    public void gossipDocument(Document doc) {
        List<String> targetNodes = getRandomNodes();
        for (String targetNodeId : targetNodes) {
            if (!targetNodeId.equals(node.getNodeId())) {
                try {
                    // send document via TCP or UDP

                    System.out.println("Gossiping document version " + doc.getVersionNumber() + " to " + targetNodeId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Gossip commit notification to a subset of nodes
    public void gossipCommit(Document doc) {
        List<String> targetNodes = getRandomNodes();
        for (String targetNodeId : targetNodes) {
            if (!targetNodeId.equals(node.getNodeId())) {
                try {
                    
                    
                    
                    System.out.println("Gossiping commit of document ID " + doc.getId() + " to " + targetNodeId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
