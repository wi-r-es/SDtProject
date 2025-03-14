package Testing;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;


import Nodes.Node;
import Resources.Document;
import remote.messageRemoteInterface;
import shared.OPERATION;

public class Client {
    
    public static void main(String[] args) throws InterruptedException {
        try {
            printOptions();
            
            System.out.println(Naming.list("rmi://localhost:2323").toString());
            messageRemoteInterface rq = (messageRemoteInterface) Naming.lookup("rmi://localhost:2323/MessageQueue");
            System.out.println("Connected to MessageQueue");

            Document doc1 = new Document("This is a new document1");
            Document doc2 = new Document("This is a new document2");
            Document doc3 = new Document("This is a new document3");

            // Perform remote operations
            //rq.enqueue(msg);
            rq.enqueue(rq.performOperation(OPERATION.CREATE, doc1));
            rq.enqueue(rq.performOperation(OPERATION.UPDATE, doc1));
            rq.enqueue(rq.performOperation(OPERATION.CREATE, doc2));
            rq.enqueue(rq.performOperation(OPERATION.CREATE, doc2));
            rq.enqueue(rq.performOperation(OPERATION.CREATE, doc3));
            rq.enqueue(rq.performOperation(OPERATION.DELETE, doc3));
            Thread.sleep(10000);
            Node node = new Node("Node-NEW");
            node.start();
            node.getGossipNode().getHeartbeatService().syncNewElement();

            Document doc4 = new Document("This is a new document4");
            Document doc5 = new Document("This is a new document5");
            Document doc6 = new Document("This is a new document6");
            doc1.setContent("Updated doc 1");

            rq.enqueue(rq.performOperation(OPERATION.CREATE, doc4));
            rq.enqueue(rq.performOperation(OPERATION.UPDATE, doc1));
            rq.enqueue(rq.performOperation(OPERATION.CREATE, doc5));
            rq.enqueue(rq.performOperation(OPERATION.CREATE, doc6));
            rq.enqueue(rq.performOperation(OPERATION.DELETE, doc2));
        } catch (MalformedURLException | RemoteException | NotBoundException e) {
            e.printStackTrace();
        } 
    }

    public static void printOptions(){
        try{ 
        String [] opts = Naming.list("rmi://localhost:2323/Node-0");
        for(int i=0; i<opts.length; i++){
             System.out.println(opts[i]);
        }
    } catch( Exception e){
        e.printStackTrace();
    }
     }
}


