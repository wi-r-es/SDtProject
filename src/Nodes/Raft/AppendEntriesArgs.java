package Nodes.Raft;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
/**
 * Represents the arguments for the AppendEntries RPC in the Raft consensus algorithm.
 * @see Nodes.Raft.LogEntry
 */
public class AppendEntriesArgs  implements Serializable {
    private final int term;                   // Leader's term
    private final UUID  leaderId;             // Leader's ID
    private final int prevLogIndex;           // Index of log entry immediately preceding new entries
    private final int prevLogTerm;            // Term of prevLogIndex entry
    private final int leaderCommit;           // Leader's commitIndex
    private final List<LogEntry> entries;     // Log entries to store

    /**
     * Constructs an instance of AppendEntriesArgs.
     *
     * @param term         The leader's term.
     * @param leaderId     The leader's ID.
     * @param prevLogIndex The index of the log entry immediately preceding new entries.
     * @param prevLogTerm  The term of the prevLogIndex entry.
     * @param entries      The log entries to store.
     * @param leaderCommit The leader's commitIndex.
     */
    public AppendEntriesArgs(int term, UUID leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }
    // Getters for the fields
    public int getTerm() {
        return term;
    }

    public UUID getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    public String toString() {
        return "AppendEntriesArgs{" +
                "term=" + term +
                ", leaderId=" + leaderId +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", leaderCommit=" + leaderCommit +
                ", entries=" + entries +
                '}';
    }
}

