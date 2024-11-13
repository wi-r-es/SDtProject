import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import remote.messageRemote;

public class Client {
    public static void main(String[] args) {
        try {
            messageRemote queue = (messageRemote) Naming.lookup("rmi://localhost:9999/MessageQueue");
            System.out.println("Connected to MessageQueue");
            // Now you can invoke remote methods on messageQueue
        } catch (MalformedURLException | RemoteException | NotBoundException e) {
            e.printStackTrace();
        }
        
        
    }
}
