package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

import shared.MessageQueue;

public interface messageRemoteInterface extends Remote {
    
    public void enqueue(String message) throws RemoteException;;
    public String dequeue() throws RemoteException;;
}
