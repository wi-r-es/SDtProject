package Nodes.Raft;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


import shared.Message;
import shared.OPERATION;
import Nodes.Node;

/**
 * The RaftNode class represents a node in a Raft consensus cluster.
 * It extends the Node class and implements the Raft consensus algorithm and needed variables.
 */
public class RaftNode extends Node {
    //FOR LOGS
    //private static final Logger LOGGER = Logger.getLogger(RaftNode.class.getName());
    private List<LogEntry> log;
    //CONSTANTS
    private static final int ELECTION_TIMEOUT_MIN = 150;  
    private static final int ELECTION_TIMEOUT_MAX = 300;  
    private static final int HEARTBEAT_INTERVAL = 1000;     


    private AtomicInteger currentTerm;
    //private UUID votedFor;
    private final AtomicReference<UUID> votedFor;
    
    private final AtomicReference<NodeState> state;
    private UUID leaderId;
    private Set<UUID> votesReceived;
    //private Timer electionTimer;
    
    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> electionMonitor;  // For leader election
    private volatile ScheduledFuture<?> heartbeatTimer;
    // For leader election
    private final AtomicLong electionTimeout;  // Track when election should happen
    private final Random random;
    private final Object timerLock = new Object();

    /**
     * Constructor for the RaftNode class.
     *
     * @param nodeId    The ID of the node.
     * @param isLeader  Indicates if the node is the leader.
     * @throws RemoteException If a remote exception occurs.
     */
    public RaftNode(String nodeId, boolean isLeader) throws RemoteException {
        super(nodeId, isLeader);
        this.currentTerm = new AtomicInteger(0);;
        this.log = new ArrayList<>();
        this.state = new AtomicReference<>(NodeState.FOLLOWER);
        this.leaderId = null;
        this.votedFor = new AtomicReference<>(null);
        this.votesReceived = ConcurrentHashMap.newKeySet();
        //this.electionTimer = new Timer(true);
        this.scheduler = Executors.newScheduledThreadPool(2); // VS newVirtualThreadPerTaskExecutor
        this.random = new Random();
        this.electionTimeout = new AtomicLong(0);
        startElectionMonitor();
    }

    /**
     * Returns the current state of the node.
     *
     * @return The current state of the node.
     */
    public NodeState getNodeState() {
        return state.get();
    }
    /**
     * Returns the current term of the node.
     *
     * @return The current term of the node.
     */
    public int getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Shuts down the node.
     */
    public void shutdown() {
        scheduler.shutdownNow();
    }

    /**
     * Adds a new log entry.
     *
     * @param logType The type of the log entry.
     * @param vars    The variables associated with the log entry.
     */
    private void addNewLog(String logType, Object... vars){
        LogEntry newLog = null ;
        switch(logType){
            case "ELECTION":
                newLog = new LogEntry(currentTerm.get(), log.size(), 
                String.format("Node [%s]:[%s] starting election for term %d", this.getNodeName(), this.getNodeId().toString(), currentTerm.get()));
                break;
            case "VOTE_RESPONSE":
            if(vars.length == 1)
                {newLog = new LogEntry(currentTerm.get(), log.size(), 
                String.format("Node [%s]:[%s] Handling Vote request: %s", this.getNodeName(), this.getNodeId().toString(), vars));
                break;}
            case "BECOMING_LEADER":
                newLog = new LogEntry(currentTerm.get(), log.size(), 
                String.format("Node [%s]:[%s] becoming leader for term %d", this.getNodeName(), this.getNodeId().toString(), currentTerm.get()));
                break;
            case "RESET":
                if(vars.length == 1){
                    //System.out.println("Class:"+ vars[0].getClass());
                    if(vars[0] instanceof Integer ) {
                        int  timeout = (int)vars[0];
                        
                        newLog = new LogEntry(currentTerm.get(), log.size(), 
                        String.format("Node [%s]:[%s] reset election timeout to %dms from now", this.getNodeName(), this.getNodeId().toString(), timeout));
                            
                        break;
                    }
                }
            
            default:
                return ;
        }
        if(newLog != null)
            {log.add(newLog);}
    }

    /**
     * The run method is executed when the thread starts.
     * It initializes the Raft state and starts the election monitor.
     * Then it calls the parent's method.
     */
    @Override
    public void run() {
        try {
            // Initialize Raft state
            state.set(NodeState.FOLLOWER);
            startElectionMonitor();  // Starts the continuous election timeout checking
            resetElectionTimeout();  // Sets initial random timeout

            
            super.run(); // The parent run method  will use overridden methods due to polymorphism aint that F'ing neat bruh
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Clean up Raft-specific resources
            if (electionMonitor != null) {
                electionMonitor.cancel(false);
            }
            if (heartbeatTimer != null) {
                heartbeatTimer.cancel(false);
            }
            scheduler.shutdownNow();
        }
    }
    /**
     * Starts the leader services when the node becomes the leader.
     */  
    @Override
    protected void startLeaderServices() {
        // Called when node becomes leader
        if (state.get() == NodeState.LEADER) {
            try {
                super.startLeaderServices();
            } catch (RemoteException e) {
                e.printStackTrace();
            }
        
        }
    }
     /**
     * Checks the message queue when the node is the leader.
     *
     * @return true if there are messages in the queue, false otherwise.
     * @throws RemoteException If a remote exception occurs.
     */
    @Override
    protected boolean checkQueue() throws RemoteException {
        // Your queue checking logic that's Raft-aware
        if (state.get() == NodeState.LEADER) {
            return super.checkQueue();
        }
        return false;
    }
    /**
     * Processes and commits messages when the node is the leader.
     */
    @Override
    protected void processAndCommit() {
        // Your processing logic that's Raft-aware
        if (state.get() == NodeState.LEADER) {
            super.processAndCommit();
        }
    }
    /**
     * Returns the log entries as a string.
     *
     * @return The log entries as a string.
     */
    public synchronized String getLogsAsString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Log Entries for Node ").append(getNodeName()).append(":\n");
        
        new ArrayList<>(log).forEach(entry -> 
            sb.append(String.format("Term: %d, Index: %d,Comand: %s%n", 
                entry.getTerm(), entry.getIndex(), entry.getCommand())));
                
        return sb.toString();
    }
    /**
     * Prints the known nodes and the current Raft state aswell as the log entries.
     */
    @Override
    protected void printKnownNodes() {
        super.printKnownNodes();
        System.out.println("Current Raft[" +getNodeName()+ "]State: " + state.get());
        System.out.println("Current Term: " + currentTerm.get());
        
        getLogsAsString();
    }
    /**
     * Checks if the node is the leader.
     *
     * @return true if the node is the leader, false otherwise.
     */
    @Override
    public boolean isLeader(){
        if(state.get() == NodeState.LEADER)
        {return true;}
        else return false;
    }
    /**
     * Starts the election monitor.
     * 
     * The election monitor is responsible for continuously checking if the election timeout has been reached.
     * If the node is not the leader and the election timeout is reached, it starts a new election.
     * 
     * The election monitor runs in a separate thread and is scheduled using a ScheduledExecutorService.
     * It starts with an initial random delay and then runs periodically every 10 milliseconds.
     * 
     * The election timeout is determined by the ELECTION_TIMEOUT_MIN and ELECTION_TIMEOUT_MAX constants,
     * which define the minimum and maximum values for the timeout, respectively.
     * The actual timeout value is randomly generated within this range.
     */
    private void startElectionMonitor() {
        int initialDelay = random.nextInt(ELECTION_TIMEOUT_MAX);
        // Start a single timer that checks election timeout continuously
        electionMonitor = scheduler.scheduleAtFixedRate(() -> {
            if (System.currentTimeMillis() >= electionTimeout.get() && 
                state.get() != NodeState.LEADER) {
                startElection();
            }
        }, initialDelay, 10, TimeUnit.MILLISECONDS);  // Start with random delay
    }
    /**
     * Resets the election timeout.
     */
    private void resetElectionTimeout() {
         // Make timeout more random by using different ranges for different nodes
        String[] parts = this.getNodeName().split("-");
        int nodeNum = Integer.parseInt(parts[1]);
        
        // Use node number to create different ranges for different nodes
        int minTimeout = ELECTION_TIMEOUT_MIN + (nodeNum * 50);  // Spread out the minimum times
        int maxTimeout = minTimeout + (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);
        
        int randomTimeout = minTimeout + random.nextInt(maxTimeout - minTimeout + 1);
        
        electionTimeout.set(System.currentTimeMillis() + randomTimeout);
        addNewLog("RESET", randomTimeout);
    }
    
    /**
     * Schedules the election timeout.
     */
    public void scheduleElectionTimeout() {
        Random random = new Random();
        String parts[] = this.getNodeName().split("-");
        int id = Integer.parseInt(parts[1]);
        int electionTimeout =  random.nextInt(150) + 200 + (id % 50);
        ScheduledExecutorService executorService= Executors.newScheduledThreadPool(1);;
        executorService.schedule(this::startElection, electionTimeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Starts a new election.
     * 
     * This method is called when the election timeout is reached and the node is not the leader.
     * It transitions the node to the candidate state and starts the election process.
     * 
     * The election process consists of the following steps:
     * 1. Increment the current term.
     * 2. Vote for self.
     * 3. Reset the election timeout.
     * 4. Send RequestVote RPCs to all other nodes.
     * 5. Wait for votes from a majority of nodes.
     * 
     * If the node receives votes from a majority of nodes, it becomes the leader.
     * If it doesn't receive enough votes or encounters a higher term, it steps down and becomes a follower.
     * 
     * The method uses a CountDownLatch to wait for the votes asynchronously.
     * It waits for a maximum of ELECTION_TIMEOUT_MIN milliseconds for the votes to arrive.
 */
    private void startElection() {
        if (state.get() == NodeState.LEADER) {
            return;
        }
        // Reset timeout immediately when starting election
        resetElectionTimeout();

        // Transition to candidate state
        state.set(NodeState.CANDIDATE);
        currentTerm.incrementAndGet();
        votedFor.set(getNodeId()); // Vote for self 
        votesReceived.add(getNodeId());  // Include self vote

        addNewLog("ELECTION");

        Set<Map.Entry<UUID, Integer>> peers = getKnownNodes();
        CountDownLatch voteLatch = new CountDownLatch(peers.size());
        

        // Request votes from all peers
        /*
                        
                        //AtomicInteger voteCount = new AtomicInteger(1); 
                        // Create vote request args
                        RequestVoteArgs voteArgs = new RequestVoteArgs(currentTerm.get(), getNodeId(), log.size() - 1, getLastLogTerm());
                        // Request votes from all peers
                        for (Map.Entry<UUID, Integer> peer : peers) {
                            if (!peer.getKey().equals(getNodeId())) {
                                scheduler.execute(() -> {
                                    Message voteRequest = new Message(
                                        OPERATION.VOTE_REQ,
                                        voteArgs
                                    );
                                    
                                    this.getGossipNode().getHeartbeatService().sendUncompMessage(
                                        voteRequest,
                                        peer.getKey(),
                                        getPeerPort(peer.getKey())
                                    );
                                    voteLatch.countDown();
                                });
                            } else {
                                voteLatch.countDown(); // Count down for self
                            }
                        }
        */
        //or simply multicast
        sendVoteRequest();

        try {
            voteLatch.await(ELECTION_TIMEOUT_MIN, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
    }
    /**
     * Retries the election.
     * 
     * This method is called when the node is a candidate and a split vote occurs (no candidate receives a majority of votes).
     * It increments the current term, adds a random delay, and starts a new election.
     * 
     * The random delay is added to prevent multiple nodes from starting a new election simultaneously,
     * which could lead to repeated split votes. The delay is calculated based on the ELECTION_TIMEOUT_MIN,
     * ELECTION_TIMEOUT_MAX, and a node-specific factor(its number counterpart from its name).
     */
    private void retryElection() {
        if (state.get() == NodeState.CANDIDATE) {
            System.out.println("[DEBUG] Node " + getNodeName() + " restarting election for term " + (currentTerm.incrementAndGet()));
            // Add more randomization to retry delay
            int retryDelay = ELECTION_TIMEOUT_MIN + 
            random.nextInt(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) +
            (Integer.parseInt(getNodeName().split("-")[1]) * 20);  // Add node-specific delay
            try {
                Thread.sleep(retryDelay); 
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            startElection();
        }
    }


    /**
     * Sends a vote request to all known nodes.
     */
    private void sendVoteRequest() { // in a perfect world should send this via unicast to every other node instead of multicasting
        Message voteRequest = new Message( //public Message(OPERATION op, Object pl, String nodeName, UUID nodeId, int udpPort) {
            OPERATION.VOTE_REQ,
            new RequestVoteArgs(currentTerm.get(), getNodeId(), log.size() - 1, getLastLogTerm()),
            getNodeName(),
            getNodeId(),
            getGossipNode().getHeartbeatService().getUDPport()
        );
        System.out.println(voteRequest);
        System.out.println("[DEBUG] Sending vote request to peers." );
        this.getGossipNode().getHeartbeatService().broadcast(voteRequest, false);
        
    }
    /**
     * Returns the term of the last log entry.
     *
     * @return The term of the last log entry.
     */
    private int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    /**
     * Checks if the log is up to date.
     *
     * @param lastLogIndex The index of the last log entry.
     * @param lastLogTerm  The term of the last log entry.
     * @return true if the log is up to date, false otherwise.
     */
    private boolean isLogUpToDate(int lastLogIndex, int lastLogTerm) {
        int myLastLogTerm = getLastLogTerm();
        return (lastLogTerm > myLastLogTerm) ||
                (lastLogTerm == myLastLogTerm && lastLogIndex >= log.size() - 1);
    }
    /**
     * Handles a vote request from a candidate.
     *
     * This method is called when the node receives a RequestVote RPC from a candidate.
     * It checks if the candidate's term is higher than the node's current term and updates the node's state accordingly.
     * 
     * The node grants its vote to the candidate if all the following conditions are met:
     * 1. The candidate's term is equal to or greater than the node's current term.
     * 2. The node has not voted for another candidate in the current term.
     * 3. The candidate's log is at least as up-to-date as the node's log.
     * 4. The node is not currently a candidate.
     * 
     * If the node grants its vote, it updates its state and resets the election timeout.
     * It then sends a RequestVoteReply to the candidate indicating whether the vote was granted or not.
     *
     * @param args The arguments of the RequestVote RPC.
     * @param port The port of the candidate node.
     */
    public synchronized void handleVoteRequest(RequestVoteArgs args, int port) {
        System.out.println("[DEBUG] Hadnling Vote request." );
        System.out.println(args);
        addNewLog("VOTE_RESPONSE", args);

        // If candidate's term is higher, update local term and become follower
        if (args.getTerm() > currentTerm.get()) {
            currentTerm.set(args.getTerm());
            state.set(NodeState.FOLLOWER);
            votedFor.set(null);
        }

        boolean voteGranted = false;

        /* Only grant vote if:
            1. Term is current
            2. Haven't voted for anyone else in this term
            3. Candidate's log is up to date
            4. We're not already in an election as a candidate
        */
        if (args.getTerm() == currentTerm.get() &&
            (votedFor.get() == null || votedFor.get().equals(args.getCandidateId())) &&
            isLogUpToDate(args.getLastLogIndex(), args.getLastLogTerm()) &&
            state.get() != NodeState.CANDIDATE) {    // Don't vote if we're a candidate
            
            votedFor.set(args.getCandidateId());
            voteGranted = true;
            //startElectionTimer(); // Reset timeout since we acknowledge a leader
            resetElectionTimeout(); // Reset timeout since we acknowledge a leader
        }

        // Create Vote response
        RequestVoteReply reply = new RequestVoteReply(
            currentTerm.get(),
            voteGranted,
            getNodeId()
        );

        Message voteResponse = new Message(
            OPERATION.VOTE_ACK,
            reply
        );

        // Send response back to candidate
        this.getGossipNode().getHeartbeatService().sendUncompMessage(
            voteResponse,
            args.getCandidateId(),
            port//getPeerPort(args.getCandidateId())
        );
    }
    /**
     * Handles a vote response from a node.
     *
     * This method is called when the node receives a RequestVoteReply from another node in response to its vote request.
     * 
     * If the node is a candidate and the reply is for the current term, it processes the vote response.
     * If the vote is granted, the node adds the voter's ID to its vote count.
     * If the vote count reaches a majority, the node becomes the leader.
     * 
     * If the reply indicates a higher term, the node updates its current term and transitions to the follower state.
     * 
     * If the node is still a candidate after processing the vote response and it did not receive a majority of votes,
     * it retries the election.
     *
     * @param reply The RequestVoteReply received from the node.
     */
    public synchronized void handleVoteResponse(RequestVoteReply reply) {
        System.out.println("[DEBUG] [node: "+ this.getNodeName()+ "]Received vote response from " + reply.getVoterId() +
                       " for term " + reply.getTerm() + ": vote granted = " + reply.isVoteGranted());
        if (state.get() != NodeState.CANDIDATE || reply.getTerm() != currentTerm.get()) return;

        if (reply.isVoteGranted()) {
            votesReceived.add(reply.getVoterId());
            System.out.println("known nodes: " + this.getKnownNodes().size());
            if (votesReceived.size() > (getKnownNodes().size() / 2)) {
                becomeLeader();
            }
        } else if (reply.getTerm() > currentTerm.get()) {
            currentTerm.set(reply.getTerm());
            state.set(NodeState.FOLLOWER);
            votedFor.set(null);
            resetElectionTimeout();
            //resetElectionTimer();
        }

         // Check for split vote
        if (state.get() == NodeState.CANDIDATE && votesReceived.size() <= (getKnownNodes().size() / 2)) {
            retryElection(); // Retry election
        }
    }

    /**
     * Transitions the node to the leader state.
     * 
     * This method is called when the node receives a majority of votes and becomes the leader.
     * It performs the following actions:
     * 1. Sets the node's state to LEADER.
     * 2. Stops the election timeout monitor.
     * 3. Starts sending periodic heartbeats to followers.
     * 4. Initializes the nextIndex and matchIndex for each follower.
     * 5. Starts the leader-specific services.
     */
    @Override
    protected void becomeLeader() {
        if (state.get() != NodeState.CANDIDATE) {
            return;
        }
        addNewLog("BECOMING_LEADER");
        
        System.out.println("[DEBUG]: The following node is the new leader: " + getNodeName());
        state.set(NodeState.LEADER);
        
        synchronized (timerLock) {
            // Stop checking election timeout since we're now leader
            if (electionMonitor != null) {
                electionMonitor.cancel(false);
            }

            // Start sending heartbeats
            heartbeatTimer = scheduler.scheduleAtFixedRate(
                this::broadcastHeartbeat,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS
            );
        }
        super.becomeLeader();
        
    }
    /**
     * Steps down as the leader.
     *
     * @param newTerm The new term.
     */
    public void stepDown(int newTerm) {
        System.out.println("[DEBUG] Stepping down: current term=" + currentTerm.get() + ", new term=" + newTerm);
        // Called when we discover a higher term or need to step down
        currentTerm.set(newTerm);
        state.set(NodeState.FOLLOWER);
        votedFor.set(null);
        
        synchronized (timerLock) {
            
            // Stop heartbeat timer if it exists
            if (heartbeatTimer != null) {
                heartbeatTimer.cancel(false);
                heartbeatTimer = null;
            }
            
            // Restart election monitoring
            if (electionMonitor == null || electionMonitor.isCancelled()) {
                startElectionMonitor();
            }
            
            // Reset the election timeout
            resetElectionTimeout();
        }
    }
    /**
     * Broadcasts a heartbeat to all known nodes.
     */
    private void broadcastHeartbeat() { //public static Message LheartbeatMessage(String content, String nodeName, UUID nodeId, int udpPort) {
        if (state.get() != NodeState.LEADER) {
            return;
        }
        Message heartbeat = Message.LheartbeatMessage(
            "Heartbeat from leader:" + getNodeId() + ":" + currentTerm,
            getNodeName(),
            getNodeId(),
            getGossipNode().getHeartbeatService().getUDPport()
        );
        this.getGossipNode().getHeartbeatService().broadcast(heartbeat, false);
    }


    
    /**
     * Handles a heartbeat message from the leader.
     *
     * This method is called when the node receives a heartbeat message from the leader.
     * The heartbeat message contains the leader's term and the leader's node ID.
     *
     * The node processes the heartbeat message as follows:
     *
     * 1. If the heartbeat is from the node itself (i.e., the node is the leader), it ignores the heartbeat.
     *
     * 2. If the leader's term is older than the node's current term, it ignores the heartbeat.
     *
     * 3. If the leader's term is newer than the node's current term, the node updates its current term to the leader's term
     *    and transitions to the follower state. It also clears its voted-for state.
     *
     * 4. If the leader's term is equal to the node's current term and the node is not the leader:
     *    - If the node is a candidate, it means that another node has been elected as the leader for the same term.
     *      In this case, the node transitions to the follower state and acknowledges the leader.
     *    - If the node is already a follower, it updates its leader ID and resets the election timeout.
     *
     * 5. If the leader's term is equal to the node's current term and the node itself is the leader:
     *    - This is an unexpected scenario where two nodes consider themselves leaders for the same term.
     *    - To resolve this conflict, the node compares its own node ID with the leader's node ID.
     *    - If the leader's node ID is greater than the node's own ID, the node steps down and becomes a follower.
     *
     * If an invalid heartbeat message is received (e.g., invalid format or missing fields), the method logs an error
     * and ignores the heartbeat.
     *
     * By processing heartbeat messages, the node maintains its awareness of the current leader and term,
     * and transitions its state accordingly to maintain consistency with the rest of the cluster.
     *
     * @param message The heartbeat message received from the leader.
     */
    public synchronized void handleHeartbeat(Message message) {
        Object payload = message.getPayload();
        System.out.println("Handliong Heartbeat RAFT NODE");
        System.out.println(payload);
        if (!(payload instanceof String)) {
            System.err.println("[ERROR] Invalid leader heartbeat payload type");
            return;
        }
        String[] parts = ((String) payload).split(":", 3);
        if (parts.length != 3) {
            System.err.println("[ERROR] Invalid heartbeat format: " + payload);
            return;
        }
            
        try {
            UUID leaderNodeId = UUID.fromString(parts[1].trim());
            int leaderTerm = Integer.parseInt(parts[2].trim());
            
            // Ignore our own heartbeats
            if (leaderNodeId.equals(this.getNodeId())) {
                return;
            }
    
            System.out.println("[DEBUG] Received heartbeat from leader " + leaderNodeId +
                            " for term " + leaderTerm + ", current term: " + currentTerm.get());
    
            if (leaderTerm < currentTerm.get()) {
                return;  // Ignore old term heartbeats
            }
                            // If we see a higher term, always update our term and become follower
            
            if (leaderTerm > currentTerm.get()) {
                System.out.println("[DEBUG] Updating to higher term: " + leaderTerm);
                stepDown(leaderTerm);
                return;
            }
    
            // If same term and we're not the leader, acknowledge leader and reset timeout
            if (leaderTerm == currentTerm.get()) {
                if (state.get() != NodeState.LEADER) {
                    leaderId = leaderNodeId;
                    state.set(NodeState.FOLLOWER);
                    resetElectionTimeout();
                    System.out.println("[DEBUG] Acknowledged leader " + leaderNodeId + " for term " + leaderTerm);
                } else {
                    // We're leader in same term - this shouldn't happen!
                    System.out.println("[WARN] Received heartbeat from another leader in same term!");
                    // In this case, highest node ID wins to break tie
                    if (leaderNodeId.compareTo(getNodeId()) > 0) {
                        System.out.println("[DEBUG] Stepping down due to higher node ID");
                        stepDown(leaderTerm);
                    }
                }
            }
    
        } catch (IllegalArgumentException | IndexOutOfBoundsException ex) {
            System.err.println("[ERROR] Failed to parse heartbeat: " + payload);
            ex.printStackTrace();
        }
    }
    //WIP
    public synchronized void handleAppendEntries(AppendEntriesArgs args) {
        // Implementation for AppendEntries RPC/UDP (leader to followers)
    }
    
}

