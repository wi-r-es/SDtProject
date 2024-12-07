package shared;

/**
 * Represents the available operations that can be performed on messages.
 */
public enum OPERATION {
    CREATE,           // Create a new document
    UPDATE,           // Update an existing document
    DELETE,           // Delete a document
    SYNC,             // Synchronize documents between nodes
    ACK,              // Acknowledge a synchronization request
    FULL_SYNC,        // Perform a request for full synchronization of all documents
    FULL_SYNC_ANS,    // Answer a full synchronization request
    FULL_SYNC_ACK,    // Acknowledge a full synchronization answer
    COMMIT,           // Commit changes to documents (simply put commits an operation previously sent for sync)
    HEARTBEAT,        // Send a heartbeat message to check node availability
    LHEARTBEAT,       // Send a heartbeat message from the leader node
    HEARTBEAT_ACK,    // Acknowledge a heartbeat message
    DISCOVERY,        // Discover other nodes in the cluster (for leader discovery)
    DISCOVERY_ACK,    // Acknowledge a discovery request (leader reply)
    REVERT,           // Revert changes to documents (cancel sync)
    VOTE_REQ,         // Request a vote during leader election
    VOTE_ACK,         // Acknowledge a vote request
    APPEND_ENTRIES,   // Append entries to the log (log replication)
    APPEND_ENTRIES_REPLY, // Reply to an append entries request
    COMMIT_INDEX,     // Commit an index in the log (commit the log replication from APPEND_ENTRIES)
    QUEUE_TRANSFER    // Transfer ownership of the message queue
}