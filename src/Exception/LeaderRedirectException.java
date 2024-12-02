package Exception;

import java.rmi.RemoteException;
import java.util.UUID;

public class LeaderRedirectException extends RemoteException {
    private final UUID leaderId;

    public LeaderRedirectException(UUID leaderId) {
        super("Not the leader");
        this.leaderId = leaderId;
    }

    public UUID getLeaderId() {
        return leaderId;
    }
}