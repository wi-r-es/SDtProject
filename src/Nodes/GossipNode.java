package Nodes;
import Services.HeartbeatService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
}
