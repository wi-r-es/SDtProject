package Nodes.Raft;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents the reply for the AppendEntries RPC in the Raft consensus algorithm.
 */
public class AppendEntriesReply  implements Serializable {
    private final int term;        // Current term of the responding node
    private final boolean success; // Whether the entries were appended successfully
    private final UUID nodeID; // follower node ID
    private final Integer lastLogIndex; // for when log replication failed 

    /**
     * Constructs an instance of AppendEntriesReply.
     *
     * @param term    The current term of the responding node.
     * @param success Whether the entries were appended successfully.
     */
    public AppendEntriesReply(int term, boolean success, UUID id) {
        this.term = term;
        this.success = success;
        this.nodeID = id;

        this.lastLogIndex = (Integer) null;
    }
    public AppendEntriesReply(int term, boolean success, UUID id, int lastLogIndex) {
        this.term = term;
        this.success = success;
        this.nodeID = id;
        this.lastLogIndex = lastLogIndex;
    }
    
    // Getters for the fields
    public int getTerm() {
        return term;
    }
    public boolean isSuccess() {
        return success;
    }
    public UUID getnodeID() {
        return nodeID;
    }
    public Integer getlastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public String toString() {
        return "AppendEntriesReply{" +
                "term=" + term +
                ", success=" + success +
                ", nodeID=" + nodeID +
                ", lastLogIndex=" + lastLogIndex!=null ? Integer.toString(lastLogIndex) : "not defined" +
                '}';
    }
}


