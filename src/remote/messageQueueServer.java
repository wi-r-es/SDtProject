package remote;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import shared.Message;
import shared.MessageQueue;
import remote.messageRemoteInterface;

public class messageQueueServer extends Thread {       

    private Registry registry;
    private MessageQueue mq;
    private int registryPort;
    private String regName;
    
    public messageQueueServer(String NodeID, int port){
        regName = NodeID;
        registryPort = port;
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(this::unreg));
        try {
            mq = new MessageQueue();
            Registry reg = LocateRegistry.createRegistry(registryPort);
            System.out.println("RMI registry created on port " + registryPort);
            
            
            //String regURL = "rmi://localhost:" + registryPort + "/queue";
            //reg.rebind(regURL, mq);
            System.out.println(regName);
            System.out.println(mq);

            reg.rebind(regName+"/queue", mq);
            System.out.println("MessageQueue bound to registry as 'queue'");



        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

   

    public void unreg() {
        try {
            if (registry != null && mq != null) {
                registry.unbind("queue"); 
                UnicastRemoteObject.unexportObject(mq, true); 
                System.out.println("MessageQueue unbound and unexported from RMI registry");

                UnicastRemoteObject.unexportObject(registry, true); 
                System.out.println("RMI registry unexported");
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to stop RMI server", e);
        }
    }

}