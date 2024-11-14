import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import remote.messageRemoteInterface;

public class Client {
    public static void main(String[] args) {
        try {
            messageRemoteInterface queue = (messageRemoteInterface) Naming.lookup("rmi://localhost:2323/queue");
            System.out.println("Connected to MessageQueue");
            // Perform remote operations
            queue.enqueue("Hello, RMI!");
        } catch (MalformedURLException | RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
        
        
    }
}
