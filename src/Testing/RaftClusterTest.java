package Testing;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;

import Nodes.Node;
import Nodes.Raft.RaftNode;
public class RaftClusterTest {
    public static void main(String[] args) {
        List<RaftNode> nodes = new ArrayList<>();
        //nodes.add(new RaftNode("Node-0", true));
        for (int i = 0; i < 5; i++) { 
            String nodeId = "Node-" + i;
            try {
                nodes.add(new RaftNode(nodeId, false));
            } catch (RemoteException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    
        // Start all nodes
        //nodes.forEach(node -> new Thread(node::startElection).start());
        nodes.forEach(node -> new Thread(node::scheduleElectionTimeout).start());
    }
}
    



