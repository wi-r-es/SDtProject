package shared;

/**
 * Represents the available operations that can be performed on messages.
 */
public enum OPERATION {
    CREATE,
    UPDATE,
    DELETE,
    SYNC,
    ACK,
    FULL_SYNC,
    FULL_SYNC_ANS,
    FULL_SYNC_ACK,
    COMMIT,
    HEARTBEAT,
    LHEARTBEAT,
    HEARTBEAT_ACK,
    DISCOVERY,
    DISCOVERY_ACK,
    REVERT,
    VOTE_REQ,
    VOTE_ACK,
    APPEND_ENTRIES,
    APPEND_ENTRIES_REPLY,
    COMMIT_INDEX,
    QUEUE_TRANSFER
    
}