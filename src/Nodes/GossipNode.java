package Nodes;
import Services.HeartbeatService;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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

}
