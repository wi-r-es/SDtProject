package remote;

import Documents.Document;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DocumentManagementInterface extends Remote  {
    void receiveDocument(Document doc) throws RemoteException;
    void sendAckToLeader(Document doc) throws RemoteException;
    void applyCommittedVersion(Document doc) throws RemoteException;
    void processAck(Document doc) throws RemoteException;
}
