import java.rmi.RemoteException;

import Nodes.Node;

public class test {
    public static void main(String[] args) throws RemoteException {
        if (args.length == 0) {
            System.err.println("Please provide a node ID as an argument.");
            return;
        }

        String nodeId = args[0];
        Node node = new Node(nodeId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            node.unregisterFromDiscoveryServer();
            System.out.println("Node " + nodeId + " shutting down.");
        }));

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

