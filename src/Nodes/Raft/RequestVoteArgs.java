package Nodes.Raft;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents the arguments for the RequestVote RPC in the Raft consensus algorithm.
 */
public class RequestVoteArgs  implements Serializable {
    private final int term;
    private final UUID candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;

    /**
     * Constructs an instance of RequestVoteArgs.
     *
     * @param term         The term of the candidate.
     * @param candidateId  The ID of the candidate.
     * @param lastLogIndex The index of the candidate's last log entry.
     * @param lastLogTerm  The term of the candidate's last log entry.
     */
    public RequestVoteArgs(int term, UUID candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
    
    // Getters for the fields
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

    /**
     * Returns a string representation of the RequestVoteArgs.
     *
     * @return A string representation of the RequestVoteArgs.
     */
    @Override
    public String toString(){
        return "{RequestVoteArgs:{term="+term+",candidateID="+candidateId+",lastLogIndex="+lastLogIndex+",lastLogTerm="+lastLogTerm+ "}}";
    }
}

