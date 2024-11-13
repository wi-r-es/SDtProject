package remote;

import java.rmi.Remote;

import shared.Message;

public interface messageRemote extends Remote {
    public void enqueue(Message message);
}
