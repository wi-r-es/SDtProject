package Nodes.Raft;

import java.io.Serializable;
import java.util.UUID;

public class RequestVoteArgs  implements Serializable {
    private final int term;
    private final UUID candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteArgs(int term, UUID candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public UUID getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }
    public String toString(){
        return "{RequestVoteArgs:{term="+term+",candidateID="+candidateId+",lastLogIndex="+lastLogIndex+",lastLogTerm="+lastLogTerm+ "}}";
    }
}

