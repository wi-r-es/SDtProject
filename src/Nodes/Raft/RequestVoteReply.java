package Nodes.Raft;

import java.io.Serializable;
import java.util.UUID;

public class RequestVoteReply  implements Serializable {
    private final int term;
    private final boolean voteGranted;
    private final UUID voterId;

    public RequestVoteReply(int term, boolean voteGranted, UUID voterId) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.voterId = voterId;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public UUID getVoterId() {
        return voterId;
    }
    public String toString(){
        return "{RequestVoteArgs:{term="+term+",voteGranted="+voteGranted+",voterId="+voterId+ "}}";
    }
}