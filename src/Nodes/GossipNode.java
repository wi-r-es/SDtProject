package Nodes;
import Services.HeartbeatService;
import java.util.List;
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

    public String getNodeId() {
        return node.getNodeId();
    }

    public HeartbeatService getHeartbeatService() {
        return heartbeatService;
    }

    public List<String> getRandomNodes() {
        return node.getRandomNodes();
    }

}
