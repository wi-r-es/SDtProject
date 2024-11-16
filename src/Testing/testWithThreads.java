package Testing;
import java.rmi.RemoteException;

import Nodes.Node;

public class testWithThreads {
    public static void main(String[] args) throws RemoteException {
        Node node = new Node("Node-0", true);
        node.start();
        for (int i= 1; i<4; i++){
            String nodeId = "Node-" + i;
            node = new Node(nodeId);
            node.start();
        }
        
        // Keep the main thread alive to maintain the node's lifecycle
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
