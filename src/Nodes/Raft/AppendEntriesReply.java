package Nodes.Raft;

import java.io.Serializable;

public class AppendEntriesReply  implements Serializable {
    private final int term;       // Current term of the responding node
    private final boolean success; // Whether the entries were appended successfully

    public AppendEntriesReply(int term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }
}


