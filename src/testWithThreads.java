import Nodes.Node;

public class testWithThreads {
    public static void main(String[] args) {

        for (int i= 0; i<5; i++){
            String nodeId = "Node-" + i;
            Node node = new Node(nodeId);
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
