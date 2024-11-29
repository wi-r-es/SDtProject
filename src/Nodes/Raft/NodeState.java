package Nodes.Raft;

/**
 * Represents the possible states of a node in the Raft consensus algorithm.
 */
public enum NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
