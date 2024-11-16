package Testing;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

import remote.messageRemoteInterface;

public class Client {
    
    public static void main(String[] args) {
        try {
            printOptions();
            



            System.out.println(Naming.list("rmi://localhost:2323").toString());
            messageRemoteInterface rq = (messageRemoteInterface) Naming.lookup("rmi://localhost:2323/Node-0/queue");
            System.out.println("Connected to MessageQueue");
            // Perform remote operations
            rq.enqueue("Hello, RMI!");
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


