package Documents;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.UUID;

import remote.DocumentManagementInterface;

public class Document implements Serializable {
    private static final long serialVersionUID = 1L;  // Version for serialization compatibility
    private final String id;
    private String content;
    private final int versionNumber;

    public Document(String content, int versionNumber)  throws RemoteException {
        this.id = UUID.randomUUID().toString();
        this.content = content;
        this.versionNumber = versionNumber;
    }

    public String getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public int getVersionNumber() {
        return versionNumber;
    }

    @Override
    public String toString() {
        return "Document[ID=" + id + ", Version=" + versionNumber + ", Content=" + content + "]";
    }
/*
    @Override
    public void receiveDocument(Document doc) throws RemoteException {
        System.out.println("Received document version " + doc.getVersionNumber() + " for document ID " + doc.getId());
        setContent(doc.getContent());  // Update document content
        //throw new UnsupportedOperationException("Unimplemented method 'receiveDocument'");
    }

    @Override
    public void sendAckToLeader(Document doc) throws RemoteException {
        System.out.println("Sending ACK for document version " + doc.getVersionNumber() + " to the leader");
        //throw new UnsupportedOperationException("Unimplemented method 'sendAckToLeader'");
    }

    @Override
    public void applyCommittedVersion(Document doc) throws RemoteException {
        System.out.println("Applying committed version " + doc.getVersionNumber() + " of document ID " + doc.getId());
        //throw new UnsupportedOperationException("Unimplemented method 'applyCommittedVersion'");
    }

    */
}
