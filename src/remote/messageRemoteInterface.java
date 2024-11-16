package remote;

import java.rmi.Remote;
import java.rmi.RemoteException;

import shared.Message;
import shared.OPERATION;


public interface messageRemoteInterface extends Remote {
    
    public void enqueue(Message message) throws RemoteException;

    // Generic method to perform an operation
    Message performOperation(OPERATION operation, Object data) throws RemoteException;

    
    

    public Message dequeue() throws RemoteException;
    
}
