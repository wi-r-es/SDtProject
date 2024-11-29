package Nodes.Raft;

import java.io.Serializable;
import java.util.UUID;

/**
 * Represents the reply for the RequestVote RPC in the Raft consensus algorithm.
 */
public class RequestVoteReply  implements Serializable {
    private final int term;
    private final boolean voteGranted;
    private final UUID voterId;

    /**
     * Constructs an instance of RequestVoteReply.
     *
     * @param term        The term of the voter.
     * @param voteGranted Whether the vote was granted.
     * @param voterId     The ID of the voter.
     */
    public RequestVoteReply(int term, boolean voteGranted, UUID voterId) {
        this.term = term;
        this.voteGranted = voteGranted;
        this.voterId = voterId;
    }

    // Getters for the fields
    public int getTerm() {
        return term;
    }
    public boolean isVoteGranted() {
        return voteGranted;
    }
    public UUID getVoterId() {
        return voterId;
    }
    
    /**
     * Returns a string representation of the RequestVoteReply.
     *
     * @return A string representation of the RequestVoteReply.
     */
    @Override
    public String toString(){
        return "{RequestVoteArgs:{term="+term+",voteGranted="+voteGranted+",voterId="+voterId+ "}}";
    }
}