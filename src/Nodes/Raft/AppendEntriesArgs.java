package Nodes.Raft;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class AppendEntriesArgs  implements Serializable {
    private final int term;                  // Leader's term
    private final UUID  leaderId;              // Leader's ID
    private final int prevLogIndex;          // Index of log entry immediately preceding new entries
    private final int prevLogTerm;           // Term of prevLogIndex entry
    private final int leaderCommit;          // Leader's commitIndex
    private final List<LogEntry> entries;    // Log entries to store

    public AppendEntriesArgs(int term, UUID leaderId, int prevLogIndex, int prevLogTerm, List<LogEntry> entries, int leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

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
}

