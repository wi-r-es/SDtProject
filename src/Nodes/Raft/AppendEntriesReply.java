package Nodes.Raft;

import java.io.Serializable;

/**
 * Represents the reply for the AppendEntries RPC in the Raft consensus algorithm.
 */
public class AppendEntriesReply  implements Serializable {
    private final int term;        // Current term of the responding node
    private final boolean success; // Whether the entries were appended successfully

    /**
     * Constructs an instance of AppendEntriesReply.
     *
     * @param term    The current term of the responding node.
     * @param success Whether the entries were appended successfully.
     */
    public AppendEntriesReply(int term, boolean success) {
        this.term = term;
        this.success = success;
    }
    
    // Getters for the fields
    public int getTerm() {
        return term;
    }
    public boolean isSuccess() {
        return success;
    }
}


