package Nodes.Raft;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.*;



import shared.Message;
import shared.MessageQueue;
import shared.OPERATION;
import utils.UniqueIdGenerator;
import Nodes.Node;
import Resources.Document;
import remote.LeaderAwareMessageQueueServer;

/**
 * The RaftNode class represents a node in a Raft consensus cluster.
 * It extends the Node class and implements the Raft consensus algorithm and needed variables.
 * 
 * @see Logger
 * @see FileHandler
 * @see List
 * @see Map
 * @see AtomicInteger
 * @see AtomicReference
 * @see ScheduledExecutorService
 * @see ScheduledFuture
 * @see AtomicLong
 */
public class RaftNode extends Node {
    //FOR LOGS
    private static final Logger LOGGER = Logger.getLogger(RaftNode.class.getName());
    private static FileHandler fh;
    //CONSTANTS
    private static final int ELECTION_TIMEOUT_MIN = 1000;//1500;//500;//150;  
    private static final int ELECTION_TIMEOUT_MAX = 1500;//3000;//1000;//300;  
    private static final int HEARTBEAT_INTERVAL = 1000;   
    private static final long REPLICATION_TIMEOUT = 5000;  
    //fields for log replication
    private List<LogEntry> log;
    private Map<UUID, Integer> nextIndex = Collections.synchronizedMap(new HashMap<>());  // Index of next log entry to send to each node
    private Map<UUID, Integer> matchIndex = Collections.synchronizedMap(new HashMap<>()); // Index of highest log entry known to be replicated
    private volatile int commitIndex = 0;  // Index of highest log entry known to be committed
    private volatile int lastApplied = 0;  // Index of highest log entry applied to state machine
    //For Raft Node Control
    private AtomicInteger currentTerm;
    private final AtomicReference<UUID> votedFor;
    private final AtomicReference<NodeState> state;
    private UUID leaderId;
    private Set<UUID> votesReceived;
    private volatile ScheduledFuture<?> electionMonitor;  // For leader election
    private final AtomicLong electionTimeout;  // Track when election should happen
    private final Random random;
    private final Object timerLock = new Object();
    private volatile ScheduledFuture<?> heartbeatTimer;
    private ScheduledExecutorService scheduler; //for thread scheduling

    // For log replication fail for testing purposes
    private volatile boolean isSleeping = false;

    
    // public RaftNode(String nodeId, boolean isLeader) throws RemoteException {
    //     super(nodeId, isLeader);
    //     this.currentTerm = new AtomicInteger(0);;
    //     this.log = new ArrayList<>();
    //     this.nextIndex = new ConcurrentHashMap<>();     // for log replication
    //     this.matchIndex = new ConcurrentHashMap<>();    // for log replication
    //     this.commitIndex = 0;                           // log replication
    //     this.lastApplied = 0;                           // for log replication
    //     this.state = new AtomicReference<>(NodeState.FOLLOWER);
    //     this.leaderId = null;
    //     this.votedFor = new AtomicReference<>(null);
    //     this.votesReceived = ConcurrentHashMap.newKeySet();
    //     //this.electionTimer = new Timer(true);
    //     this.scheduler = Executors.newScheduledThreadPool(2); // VS newVirtualThreadPerTaskExecutor
    //     this.random = new Random();
    //     this.electionTimeout = new AtomicLong(0);
        
    //     startElectionMonitor();
    //     //initializeIndices(); // for log replication
    // }
    /**
     * Constructor for the RaftNode class.
     *
     * @param nodeId    The ID of the node.
     * @param isLeader  Indicates if the node is the leader.
     * @throws RemoteException If a remote exception occurs.
     * @see Nodes.Raft.RaftNode#startElectionMonitor()
     * @see Executors#newScheduledThreadPool()
     */
    public RaftNode(String nodeId, boolean isLeader, boolean r) throws RemoteException {
        super(nodeId, isLeader,r);
        this.currentTerm = new AtomicInteger(0);;
        this.log = new ArrayList<>();
        this.nextIndex = new ConcurrentHashMap<>();     // for log replication
        this.matchIndex = new ConcurrentHashMap<>();    // for log replication
        this.commitIndex = 0;                           // log replication
        this.lastApplied = 0;                           // for log replication
        this.state = new AtomicReference<>(NodeState.FOLLOWER);
        this.leaderId = null;
        this.votedFor = new AtomicReference<>(null);
        this.votesReceived = ConcurrentHashMap.newKeySet();
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
    public void setCurrentTerm(int newTerm) {
        currentTerm.set(newTerm);
    }

    public void appendLogEntry(LogEntry log){
        this.log.add(log);
    }
    /**
     * Returns the current commited index.
     *
     * @return The current commited index of the leader.
     */
    public int getCommitIndex() {
        return commitIndex;
    }
    /**
     * Returns the current Leader UUID.
     *
     * @return The UUID of the current leader.
     */
    public UUID getLeaderId() {
        return leaderId;
    }
    /**
     * Returns the current log entries size.
     *
     * @return The size of log.
     */
    public int getLogSize() {
        return log.size();
    }

    /**
     * Shuts down the node.
     */
    public void shutdown() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
        }
        if (electionMonitor != null) {
            electionMonitor.cancel(false);
        }
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
        }
    }

    /**
     * Adds a new loggger entry.
     *
     * @param logType The type of the loggger entry to be logged.
     * @param vars    The variables associated with the logger entry.
     */
    private void addNewLog(String logType, Object... vars){
        switch(logType){
            case "ELECTION":
                LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] starting election.", 
                                currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString()));
                
                break;
            case "VOTE_RESPONSE":
            if(vars.length == 1)
                System.out.println(vars);
                System.out.println(vars[0]);
                RequestVoteArgs rvargs = (RequestVoteArgs) vars[0];
                LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] Handling Vote request: %s", 
                                currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString(), rvargs));
                break;
            case "BECOMING_LEADER":
                LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] becoming leader.", 
                                currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString())); 
                break;
            case "RESET":
                if(vars.length == 1){
                    //System.out.println("Class:"+ vars[0].getClass());
                    if(vars[0] instanceof Integer ) {
                        int  timeout = (int)vars[0];
                        LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] reset election timeout to %dms from now.", 
                                currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString(), timeout)); 
                    }
                    break;
                }
            case "REPLICATION":
                if(vars[0] instanceof LogEntry ) {
                    LogEntry  args = (LogEntry)vars[0];
                    LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] Log Entry. Content: [%s]", 
                            currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString(), args)); 
                }
                break;
            case "APPEND_ENTRIES":
                LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] HANDLING HAPPEND ENTRIES.", 
                    currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString())); 
                break;
            case "APPEND_ENTRIES_CONTENT":
                if(vars.length == 1){
                    //System.out.println("Class:"+ vars[0].getClass());
                    if(vars[0] instanceof AppendEntriesArgs ) {
                        AppendEntriesArgs  args = (AppendEntriesArgs)vars[0];
                        LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] HANDLING HAPPEND ENTRIES. Content: [%s]", 
                                currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString(), args.toString())); 
                    }
                }
                break;
            case "APPEND_ENTRIES_REPLY":
                LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] HANDLING HAPPEND ENTRIES REPLY.", 
                    currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString())); 
                break;
            case "APPEND_ENTRIES_REPLY_CONTENT":
                if(vars.length == 1){
                    //System.out.println("Class:"+ vars[0].getClass());
                    if(vars[0] instanceof AppendEntriesReply ) {
                        AppendEntriesReply  args = (AppendEntriesReply)vars[0];
                        LOGGER.info(String.format("[Current Term]: %d -- [Log size]: %d;Node [%s]:[%s] HANDLING HAPPEND ENTRIES REPLY. Content: [%s]", 
                                currentTerm.get(), log.size() ,this.getNodeName(), this.getNodeId().toString(), args.toString())); 
                    }
                }
                break;
            default:
                return ;
        }
    }

    /**
     * The run method is executed when the thread starts.
     * It initializes the Raft state and starts the election monitor.
     * Then it calls the parent's method.
     * 
     * @see Nodes.Raft.RaftNode#startElectionMonitor();
     * @see Nodes.Raft.RaftNode#resetElectionTimeout();
     * @see Nodes.Node#run()
     * 
     */
    @Override
    public void run() {
        try {
            // Initialize Raft state
            state.set(NodeState.FOLLOWER);
            startElectionMonitor();  // Starts the continuous election timeout checking
            resetElectionTimeout();  // Sets initial random timeout

            RaftNode.fh = new FileHandler("mylog.txt");
            LOGGER.addHandler(fh); //Adds file handler to the logger
            LOGGER.setLevel(Level.ALL); // Request that every detail gets logged.
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
     * Returns the log entries as a string.
     *
     * @return The log entries as a string.
     */
    public synchronized String getLogsAsString() {
        StringBuilder sb = new StringBuilder();
        
        
        new ArrayList<>(log).forEach(entry -> {
            if (entry != null) {
                sb.append(entry.toString());
                sb.append("#");
                System.out.println("[LOGS IN MACHINE -"+getNodeName()+"] "+ entry.toString());
            }
        });
                //System.out.println("[debug full sync] "+ entry.toString())
                
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
        System.out.println("Logs: ");
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
     * Initializes two important tracking mechanisms
     * 
     * nexIndex: For each follower node, it tracks the index of the next log entry that the leader should send to that follower.
     * Initially set to the leader's log.size() (meaning start from the end of the leader's log. 
     * This optimistic approach assumes followers are up-to-date initially.
     * 
     * matchIndex: Tracks the highest log entry known to be replicated on each follower.
     * Initially set to 0 because we don't know what entries followers have replicated yet.
     * Gets updated as followers confirm they've replicated entries
     */
    private void initializeIndices() {
        for (Map.Entry<UUID, Integer> peer : getKnownNodes()) {
            nextIndex.put(peer.getKey(), log.size());
            matchIndex.put(peer.getKey(), 0);
        }
    }
/*
███████ ██      ███████  ██████ ████████ ██  ██████  ███    ██ 
██      ██      ██      ██         ██    ██ ██    ██ ████   ██ 
█████   ██      █████   ██         ██    ██ ██    ██ ██ ██  ██ 
██      ██      ██      ██         ██    ██ ██    ██ ██  ██ ██ 
███████ ███████ ███████  ██████    ██    ██  ██████  ██   ████ 
                                                               
 */

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
        if (electionMonitor != null && !electionMonitor.isCancelled()) {
            electionMonitor.cancel(false);
        }
        int initialDelay = random.nextInt(ELECTION_TIMEOUT_MAX);
        // Start a single timer that checks election timeout continuously
        electionMonitor = scheduler.scheduleAtFixedRate(() -> {
            try{
                if (System.currentTimeMillis() >= electionTimeout.get() && 
                    state.get() != NodeState.LEADER) {
                    startElection();
                }
            } catch (Exception e) {
                System.err.println("Error in election monitor: " + e.getMessage());
                e.printStackTrace();
            }
          
        }, initialDelay, 10, TimeUnit.MILLISECONDS);  // Start with random delay
    }
    /**
     * Resets the election timeout.
     */
    private void resetElectionTimeout() {
        resetElectionTimeout(false);
    }
    private void resetElectionTimeout(boolean afterFailure) {
        int nodeNum;
        try {
            String[] parts = this.getNodeName().split("-");
            nodeNum = parts.length > 1 ? Integer.parseInt(parts[1]) : 
                     Math.abs(this.getNodeId().hashCode() % 100);
        } catch (NumberFormatException e) {
            nodeNum = Math.abs(this.getNodeId().hashCode() % 100);
        }

        int minTimeout, maxTimeout;
        
        if (afterFailure) {
            // Use shorter timeouts after failure
            minTimeout = ELECTION_TIMEOUT_MIN / 2;
            maxTimeout = ELECTION_TIMEOUT_MAX / 2;
        } else {
            // Use normal timeouts
            minTimeout = ELECTION_TIMEOUT_MIN;
            maxTimeout = ELECTION_TIMEOUT_MAX;
        }

        // Use node number to create different ranges for different nodes
        int baseTimeout = minTimeout + (nodeNum * 50);
        int randomTimeout = baseTimeout + random.nextInt(maxTimeout - minTimeout + 1);
        
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
     * 
     * @see Nodes.Raft.RaftNode#sendVoteRequest()
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

        
        
        

        // Request votes from all peers
        /*
                        Set<Map.Entry<UUID, Integer>> peers = getKnownNodes();
                        CountDownLatch voteLatch = new CountDownLatch(peers.size());
                        
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

        // try {
        //     voteLatch.await(ELECTION_TIMEOUT_MIN, TimeUnit.MILLISECONDS);
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        //     return;
        // }
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
/*
██    ██  ██████  ████████ ███████     ██   ██  █████  ███    ██ ██████  ██      ███████ ██████  ███████ 
██    ██ ██    ██    ██    ██          ██   ██ ██   ██ ████   ██ ██   ██ ██      ██      ██   ██ ██      
██    ██ ██    ██    ██    █████       ███████ ███████ ██ ██  ██ ██   ██ ██      █████   ██████  ███████ 
 ██  ██  ██    ██    ██    ██          ██   ██ ██   ██ ██  ██ ██ ██   ██ ██      ██      ██   ██      ██ 
  ████    ██████     ██    ███████     ██   ██ ██   ██ ██   ████ ██████  ███████ ███████ ██   ██ ███████ 
                                                                                                        
 */

    /**
     * Sends a vote request to all known nodes.
     * 
     * @see Services.HeartbeatService#broadcast(Message, boolean);
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
     * 
     * @see Services.HeartbeatService#sendUncompMessage(Message, UUID, int)
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
     * 
     * @see Nodes.Raft.RaftNode#resetElectionTimeout()
     * @see Nodes.Raft.RaftNode#retryElection()
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
/*
██      ███████  █████  ██████  ███████ ██████  
██      ██      ██   ██ ██   ██ ██      ██   ██ 
██      █████   ███████ ██   ██ █████   ██████  
██      ██      ██   ██ ██   ██ ██      ██   ██ 
███████ ███████ ██   ██ ██████  ███████ ██   ██ 
                                                
 */
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
     * 
     * @see Nodes.Raft.RaftNode#startLeaderServices()
     * @see Nodes.Raft.RaftNode#initializeIndices()
     * @see Nodes.Raft.RaftNode#broadcastHeartbeat()
     */
    @Override
    protected void becomeLeader() {
        System.out.println("[DEBUG] Attempting to become leader...");
        if (state.get() != NodeState.CANDIDATE) {
            System.out.println("[DEBUG] Cannot become leader - not a candidate");
            return;
        }
        addNewLog("BECOMING_LEADER");
        
        System.out.println("[DEBUG]: The following node is the new leader: " + getNodeName());
        state.set(NodeState.LEADER);
        
        // Initialize tracking maps for all known nodes
        nextIndex.clear();
        matchIndex.clear();
        initializeIndices();

        
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
        System.out.println("[DEBUG] Starting leader services");
        super.becomeLeader();
        startLeaderServices();
        System.out.println("[DEBUG] Leader transition complete");
        
    }
    /**
     * Steps down as the leader.
     *
     * @param newTerm The new term.
     * @see Nodes.Raft.RaftNode#transferMessageQueue()
     * @see Nodes.Raft.RaftNode#stopQueueService()
     * @see Nodes.Raft.RaftNode#startElectionMonitor()
     * @see Nodes.Raft.RaftNode#resetElectionTimeout()
     */
    public void stepDown(int newTerm) {
        if (state.get() == NodeState.LEADER) {
            // Transfer queue contents before stopping service
            transferMessageQueue(leaderId, getPeerPort(leaderId));
            stopQueueService();
            // Clear tracking maps when stepping down
            nextIndex.clear();
            matchIndex.clear();
        }
        
        System.out.println("[DEBUG] Stepping down: current term=" + currentTerm.get() + 
                          ", new term=" + newTerm);
        currentTerm.set(newTerm);
        state.set(NodeState.FOLLOWER);
        votedFor.set(null);
        
        synchronized (timerLock) {
            if (heartbeatTimer != null) {
                heartbeatTimer.cancel(false);
                heartbeatTimer = null;
            }
            
            if (scheduler.isShutdown()) {
                scheduler = Executors.newScheduledThreadPool(2);
            }
            
            startElectionMonitor();
            resetElectionTimeout();
        }
    }

    /*
 ██████  ██    ██ ███████ ██    ██ ███████ 
██    ██ ██    ██ ██      ██    ██ ██      
██    ██ ██    ██ █████   ██    ██ █████   
██ ▄▄ ██ ██    ██ ██      ██    ██ ██      
 ██████   ██████  ███████  ██████  ███████ 
    ▀▀                                    
     */
    
    /**
    * Starts the leader services when the node becomes the leader.
    * @see MessageQueueServer
    */  
    @Override
    protected void startLeaderServices() {
        if (state.get() == NodeState.LEADER) {
            try {
                // Stop any existing service
                if (messageQueue != null) {
                    messageQueue.unreg();
                }
                
                System.setProperty("java.rmi.server.hostname", "localhost");
                
                // Create and start new message queue server
                messageQueue = new LeaderAwareMessageQueueServer(
                    getNodeName(), 
                    2323, 
                    getNodeId(),
                    this
                );
                messageQueue.start();
                
                // Wait for the server to start
                Thread.sleep(1000);
                
                System.out.println("[DEBUG] Leader services started successfully on port 2323");
            } catch (Exception e) {
                LOGGER.severe("Failed to start leader services: " + e.getMessage());
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
     * Getter for thje leader Queue
     * @return MessageQueue belonging to the leader instance.
     * @throws RemoteException If a remote exception occurs.
     * @see remote.LeaderAwareMessageQueueServer
     */
    public MessageQueue getLeaderQueue() throws RemoteException {
        try {
            if (state.get() == NodeState.LEADER) {
                return messageQueue.getQueue();
            } else if (leaderId != null) {
                // Try to connect to leader's queue
                Registry registry = LocateRegistry.getRegistry(getPeerPort(leaderId));
                LeaderAwareMessageQueueServer leaderQueue = 
                    (LeaderAwareMessageQueueServer) registry.lookup("MessageQueue");
                return leaderQueue.getQueue();
            }
            throw new RemoteException("No leader available");
        } catch (NotBoundException | RemoteException e) {
            throw new RemoteException("Failed to access message queue", e);
        }
    }
    /**
     * Transfers the message queue contents from the current leader to the new leader.
     * This method will be called by the stepping down leader before stopping its RMI service.
     * 
     * @param newLeaderId The ID of the new leader
     * @param newLeaderPort The port of the new leader
     * 
     * @see remote.messageQueueServer#getQueue()
     * @see Services.HeartbeatService#sendCompMessage(Message, UUID, int)
     */
    private void transferMessageQueue(UUID newLeaderId, int newLeaderPort) {
        try {
            if (messageQueue != null && messageQueue.getQueue() != null) {
                List<Message> messages = new ArrayList<>();
                
                // Drain the current queue atomically
                synchronized (messageQueue.getQueue()) {
                    while (!messageQueue.getQueue().isEmpty()) {
                        Message msg = messageQueue.getQueue().dequeue();
                        if (msg != null) {
                            messages.add(msg);
                        }
                    }
                }
    
                if (!messages.isEmpty()) {
                    Message transferMsg = new Message(
                        OPERATION.QUEUE_TRANSFER,
                        messages,
                        getNodeName(),
                        getNodeId(),
                        getGossipNode().getHeartbeatService().getUDPport()
                    );
    
                    // Send queue contents to new leader
                    getGossipNode().getHeartbeatService().sendCompMessage(
                        transferMsg,
                        newLeaderId,
                        newLeaderPort
                    );
                }
            }
        } catch (RemoteException e) {
            LOGGER.severe("Error transferring message queue: " + e.getMessage());
        }
    }



    /**
     * Stops queue service by callig the method to unregister and then setting the variable to null.
     * @see remote.messageQueueServer#unreg()
     */
    protected void stopQueueService() {
        if (messageQueue != null) {
            try {
                messageQueue.unreg();
                messageQueue = null;
            } catch (Exception e) {
                LOGGER.severe("Error stopping queue service: " + e.getMessage());
            }
        }
    }


    
    /*
                                    ██   ██ ███████  █████  ██████  ████████ ██████  ███████  █████  ████████ 
                                    ██   ██ ██      ██   ██ ██   ██    ██    ██   ██ ██      ██   ██    ██    
                                    ███████ █████   ███████ ██████     ██    ██████  █████   ███████    ██    
                                    ██   ██ ██      ██   ██ ██   ██    ██    ██   ██ ██      ██   ██    ██    
                                    ██   ██ ███████ ██   ██ ██   ██    ██    ██████  ███████ ██   ██    ██                                                                                                                                                                                
     */

    /**
     * Broadcasts a heartbeat to all known nodes.
     * 
     * @see Services.HeartbeatService#broadcast(Message, boolean)
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
     * 
     * @see Nodes.Raft.RaftNode#stepDown(int)
     * @see Nodes.Raft.RaftNode#resetElectionTimeout()
     * @see shared.Message#getPayload()
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
                //transferQueueOwnership(leaderNodeId);
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
                        //transferQueueOwnership(leaderNodeId);
                        stepDown(leaderTerm);
                    }
                }
            }
    
        } catch (IllegalArgumentException | IndexOutOfBoundsException ex) {
            System.err.println("[ERROR] Failed to parse heartbeat: " + payload);
            ex.printStackTrace();
        }
    }

/*
██████  ███████ ██████  ██      ██  ██████  █████  ████████ ██  ██████  ███    ██ 
██   ██ ██      ██   ██ ██      ██ ██      ██   ██    ██    ██ ██    ██ ████   ██ 
██████  █████   ██████  ██      ██ ██      ███████    ██    ██ ██    ██ ██ ██  ██ 
██   ██ ██      ██      ██      ██ ██      ██   ██    ██    ██ ██    ██ ██  ██ ██ 
██   ██ ███████ ██      ███████ ██  ██████ ██   ██    ██    ██  ██████  ██   ████ 
                                                                                  
 
 */




    /**
     * Sends an AppendEntries RPC to a peer node.
     *
     * @param peerId       The UUID of the peer node.
     * @param prevLogIndex The index of the log entry preceding the new entries.
     * @param entries      The list of log entries to append.
     * 
     * @see Services.HeartbeatService#sendCompMessage(Message, UUID, int)
     * @see Nodes.Raft.AppendEntriesArgs
    */
    private void sendAppendEntries(UUID peerId, int prevLogIndex, List<LogEntry> entries) {
        int prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).getTerm() : 0;
        
        AppendEntriesArgs args = new AppendEntriesArgs(
            currentTerm.get(),
            getNodeId(),
            prevLogIndex,
            prevLogTerm,
            new ArrayList<>(entries),
            commitIndex
        );

        Message appendMsg = new Message(
            OPERATION.APPEND_ENTRIES,
            args,
            getNodeName(),
            getNodeId(),
            getGossipNode().getHeartbeatService().getUDPport()
        );
        System.out.println("[DEBUG]->SENDING REPLICATING LOG");
        System.out.println("[DEBUG]->SENDING REPLICATING LOG CONTENT->" + args.toString());
        this.getGossipNode().getHeartbeatService().sendCompMessage(
            appendMsg,
            peerId,
            getPeerPort(peerId)
        );
    }
    /**
     * Handles an incoming AppendEntries RPC from the leader.
     *
     * @param args            The AppendEntriesArgs object containing the RPC arguments.
     * @param destination_port The destination port of the node sending the reply.
     * 
     * @see Nodes.Raft.RaftNode#sendAppendEntriesReply(UUID, boolean, int, int)
     * @see Nodes.Raft.RaftNode#resetElectionTimeout()
     * @see Nodes.Raft.RaftNode#appendLogEntry(LogEntry)
     * @see Nodes.Raft.RaftNode#processLogEntry(LogEntry)     
     * @see Nodes.Raft.RaftNode#applyCommittedEntries()
     * @see Nodes.Raft.AppendEntriesArgs
    */
    public synchronized void handleAppendEntries(AppendEntriesArgs args, int destination_port) {
        if (isSleeping) {
            System.out.println("[DEBUG] Node " + getNodeName() + " is sleeping, ignoring AppendEntries");
            return;
        }
        addNewLog("APPEND_ENTRIES");
        addNewLog("APPEND_ENTRIES_CONTENT", args);
        System.out.println("[DEBUGGING] handleAppendEntries");
        System.out.println("Received AppendEntriesArgs: " + args.toString());
        //Basic term check --  If the leader's term is less than ours, reject
        if (args.getTerm() < currentTerm.get()) {
            System.out.println("[DEBUGGING] handleAppendEntries:    1ST IF");
            sendAppendEntriesReply(args.getLeaderId(), false, currentTerm.get(), destination_port);
            return;
            
        }

        // Update term if needed. (If we receive a higher term)
        if (args.getTerm() > currentTerm.get()) {
            System.out.println("[DEBUGGING] handleAppendEntries:    2ND IF");
            currentTerm.set(args.getTerm());
            state.set(NodeState.FOLLOWER);
            votedFor.set(null);
        }

        // Reset election timeout as we've heard from current leader
        resetElectionTimeout();
        
        // Verify previous log entry
        if (args.getPrevLogIndex() >= 0) {
            System.out.println("[DEBUGGING] handleAppendEntries:    3RD IF");
            if (args.getPrevLogIndex() >= log.size()) {
                System.out.println("[DEBUGGING] handleAppendEntries:    3RD-1ST IF");
                // Don't have the previous entry - ask for retransmission - reply false 
                System.out.println("[DEBUG] Missing previous log entry. Our log size: " + 
                                    log.size() + ", requested prev index: " + args.getPrevLogIndex());
                int index = log.size()==0 ? -1 : log.size() -1; // IF log is empty it has none, if log isnt empty will return the last index (size - 1)
                sendAppendEntriesReply(args.getLeaderId(), false, currentTerm.get(), destination_port, index);
                return;
            }
            // Check if terms match at prevLogIndex
            LogEntry prevLogEntry = log.get(args.getPrevLogIndex());
            if (prevLogEntry.getTerm() != args.getPrevLogTerm()) {
                System.out.println("[DEBUGGING] handleAppendEntries:    3RD-2ND IF");
                System.out.println("[DEBUG] Term mismatch at index " + args.getPrevLogIndex() + 
                                    ". Our term: " + prevLogEntry.getTerm() + 
                                    ", leader's term: " + args.getPrevLogTerm());
                // Term mismatch in previous entry - remove this and all subsequent entries - reply false
                // Delete this entry and all that follow it 
                log = new ArrayList<>(log.subList(0, args.getPrevLogIndex()));
                sendAppendEntriesReply(args.getLeaderId(), false, currentTerm.get(), destination_port);
                return;
            }
        }

        // Process new entries
        for (int i = 0; i < args.getEntries().size(); i++) {
            LogEntry newEntry = args.getEntries().get(i);
            int entryIndex = args.getPrevLogIndex() + 1 + i;

            if (entryIndex < log.size()) {
                System.out.println("[DEBUGGING] handleAppendEntries:    IF-INSIDEFOR");
                // Check if existing entry conflicts with new one
                if (log.get(entryIndex).getTerm() != newEntry.getTerm()) {
                    System.out.println("[DEBUGGING] handleAppendEntries:    2ND IF-INSIDEFOR IF");
                    // Delete this and all following entries
                    log = new ArrayList<>(log.subList(0, entryIndex));
                    System.out.println("[DEBUGGING] Appending Log entry handleAppendEntries:    2ND IF-INSIDEFOR IF");
                    appendLogEntry(newEntry);
                }
            // If terms match, keep existing entry
            } else {
                System.out.println("[DEBUGGING] handleAppendEntries:    ELSE INSIDE FOR");
                System.out.println("[DEBUGGING] Appending Log entry handleAppendEntries:    ELSE INSIDE FOR");
                // Append new entry
                appendLogEntry(newEntry);
            }

            // Process the command in the log entry
            System.out.println("[DEBUGGING] handleAppendEntries:    GOING TO PROCESS LOGENTRY");
            processLogEntry(newEntry);
        }

        // Update commit index
        if (args.getLeaderCommit() > commitIndex) {
            commitIndex = Math.min(args.getLeaderCommit(), log.size() - 1);
            applyCommittedEntries();
        }

        // Send successful reply
        sendAppendEntriesReply(args.getLeaderId(), true, currentTerm.get(), destination_port);
    }

   /**
    * Sends an AppendEntries reply to the leader.
    *
    * @param leaderId         The UUID of the leader node.
    * @param success          Indicates whether the AppendEntries was successful.
    * @param term             The current term of the node.
    * @param destination_port The destination port of the leader.
    *
    * @see Services.HeartbeatService#sendUncompMessage(Message, int, int)
    * @see Nodes.Raft.AppendEntriesReply
    */
    private void sendAppendEntriesReply(UUID leaderId, boolean success, int term, int destination_port) {
        AppendEntriesReply reply = new AppendEntriesReply(term, success, getNodeId());
        Message replyMsg = new Message(
            OPERATION.APPEND_ENTRIES_REPLY,
            reply,
            getNodeName(),
            getNodeId(),
            getGossipNode().getHeartbeatService().getUDPport()
        );
        System.out.println("[DEBUG]: SENDING ENTRIES REPLY");
        System.out.println("Contenent-> " + replyMsg.toString());
        
        System.out.println("Sending AppendEntriesReply: " + reply);
        getGossipNode().getHeartbeatService().sendUncompMessage(
            replyMsg,
            leaderId,
            destination_port
        );
    }
   /**
    * Sends an AppendEntries reply to the leader for missing log entries.
    *
    * @param leaderId         The UUID of the leader node.
    * @param success          Indicates whether the AppendEntries was successful.
    * @param term             The current term of the node.
    * @param destination_port The destination port of the leader.
    * @param lastLogIndex     The index of the last log entry.
    *
    * @see Services.HeartbeatService#sendUncompMessage(Message, int, int)
    * @see Nodes.Raft.AppendEntriesReply
    */
    private void sendAppendEntriesReply(UUID leaderId, boolean success, int term, int destination_port, int lastLogIndex) {
        AppendEntriesReply reply = new AppendEntriesReply(term, success, getNodeId(), lastLogIndex);
        Message replyMsg = new Message(
            OPERATION.APPEND_ENTRIES_REPLY,
            reply,
            getNodeName(),
            getNodeId(),
            getGossipNode().getHeartbeatService().getUDPport()
        );
        System.out.println("[DEBUG]: SENDING ENTRIES REPLY");
        System.out.println("Contenent-> " + replyMsg.toString());
        
        System.out.println("Sending AppendEntriesReply: " + reply);
        getGossipNode().getHeartbeatService().sendUncompMessage(
            replyMsg,
            leaderId,
            destination_port
        );
    }


   /**
    * Handles an incoming AppendEntries reply from a follower.
    *
    * @param reply The AppendEntriesReply object containing the reply data.
    *
    * @see Nodes.Raft.AppendEntriesReply
    * @see Nodes.Raft.RaftNode#updateFollowerIndices(UUID)
    * @see Nodes.Raft.RaftNode#checkAndSendCommit()
    * @see Nodes.Raft.RaftNode#handleFailedAppendEntries(AppendEntriesReply)
    * @see Nodes.Raft.RaftNode#decrementNextIndex(UUID)
    */
    public synchronized void handleAppendEntriesReply(AppendEntriesReply reply) {
        addNewLog("APPEND_ENTRIES_REPLY");
        addNewLog("APPEND_ENTRIES_REPLY_CONTENT", reply);
        if (state.get() != NodeState.LEADER) {
            return;
        }
        System.out.println("[DEBUG]->handleAppendEntriesReply-> reply content: " + reply.toString());
        System.out.println("[DEBUG]->handleAppendEntriesReply-> reply LASTLOGCONDITION: " + reply.getlastLogIndex()!=  null);

        System.out.println("[DEBUG]->handleAppendEntriesReply-> Before update - nextIndex: " + nextIndex.toString());
        System.out.println("[DEBUG]->handleAppendEntriesReply-> Before update - matchIndex: " + matchIndex.toString());
        // The log replication was succesfull
        if (reply.isSuccess()) {
            updateFollowerIndices(reply.getnodeID());
            System.out.println("[DEBUG]->handleAppendEntriesReply-> After update - nextIndex: " + nextIndex);
            System.out.println("[DEBUG]->handleAppendEntriesReply-> After update - matchIndex: " + matchIndex);
            checkAndSendCommit();
        } else { // The log replication wasn't succesfull, and the follower doesn't have the logs up-to-date
            if(!reply.isSuccess() && reply.getlastLogIndex()!= null){
                System.out.println("[DEBUG]->handleAppendEntriesReply-> unsucessfull and getlastLogIndex isnt null");
                handleFailedAppendEntries(reply);
            }
            else{
                // The log replication wasn't succesfull, decrement nextIndex and retry
                System.out.println("[DEBUG]->handleAppendEntriesReply-> unsucessfull");
                System.out.println("[DEBUG]->handleAppendEntriesReply->  - nextIndex: "  + nextIndex.get(reply.getnodeID()));
                System.out.println("[DEBUG]->handleAppendEntriesReply->  - matchIndex: " + matchIndex.get(reply.getnodeID()));
                decrementNextIndex(reply.getnodeID());
            }
        }
    }

   /**
    * Handles a failed AppendEntries request by sending missing entries to the follower.
    *
    * @param reply The AppendEntriesReply object containing the follower's last log index.
    *
    * @see Nodes.Raft.RaftNode#sendAppendEntries(UUID, int, List)
    */
    private void handleFailedAppendEntries(AppendEntriesReply reply) {
        int lastLogIndex = reply.getlastLogIndex();
        UUID followerId = reply.getnodeID();
        
        if (lastLogIndex == -1) {
            // Follower has empty log - send all entries
            System.out.println("[DEBUG] Follower has empty log, sending all entries");
            sendAppendEntries(followerId, -1, new ArrayList<>(log)); // Already checked in the beginning of sendAppendEntries whether the parameter is >= 0 and different approaches for both occasions
        } else {
            // Follower has some entries - send all entries after lastLogIndex
            System.out.println("[DEBUG] Follower last log index: " + lastLogIndex);
            List<LogEntry> entriesToSend = log.subList(lastLogIndex + 1, log.size());
            sendAppendEntries(followerId, lastLogIndex, entriesToSend);
        }
    }
    
   /**
    * Checks if a majority of followers have replicated the current log index and sends a commit message if so.
    *
    * @see Nodes.Node#getKnownNodes()
    * @see Services.HeartbeatService#broadcast(Message, boolean)
    */
    private void checkAndSendCommit() {
        int matchCount = 1; // Count self
        int currentIndex = log.size() - 1;
        
        for (Integer matchIdx : matchIndex.values()) {
            if (matchIdx >= currentIndex) {
                matchCount++;
            }
        }
        
        if (matchCount > getKnownNodes().size() / 2) {
            System.out.println("[DEBUG]->handleAppendEntriesReply gonna send commit");
            System.out.println("[DEBUG] Achieved majority for index " + currentIndex + 
                ". Sending commit message.");
            commitIndex = currentIndex;
            Message commitMsg = new Message(
                OPERATION.COMMIT_INDEX,
                commitIndex,
                getNodeName(),
                getNodeId(),
                getGossipNode().getHeartbeatService().getUDPport()
            );
            System.out.println("Commit index for index: " + commitIndex);
            this.getGossipNode().getHeartbeatService().broadcast(commitMsg, true);
        }
    }
    /*
    @Deprecated
    public synchronized void handleAppendEntriesReply(AppendEntriesReply reply) {
        addNewLog("APPEND_ENTRIES_REPLY");
        addNewLog("APPEND_ENTRIES_REPLY_CONTENT", reply);
        System.out.println("[DEBUG]->handleAppendEntriesReply");
        if (state.get() != NodeState.LEADER) {
            return;
        }
        System.out.println("[DEBUG]->handleAppendEntriesReply-> reply content: " + reply.toString());
        System.out.println("[DEBUG]->handleAppendEntriesReply-> reply LASTLOGCONDITION: " + reply.getlastLogIndex()!=  null);

        System.out.println("[DEBUG]->handleAppendEntriesReply-> Before update - nextIndex: " + nextIndex.toString());
        System.out.println("[DEBUG]->handleAppendEntriesReply-> Before update - matchIndex: " + matchIndex.toString());
    
        if (reply.isSuccess()) {
            // Update indices for the successful follower
            updateFollowerIndices(reply.getnodeID());
            System.out.println("[DEBUG]->handleAppendEntriesReply-> After update - nextIndex: " + nextIndex);
            System.out.println("[DEBUG]->handleAppendEntriesReply-> After update - matchIndex: " + matchIndex);
            // Check if we have majority and can commit
            int matchCount = 1; // Count self
            int currentIndex = log.size() - 1;
            
            for (Integer matchIdx : matchIndex.values()) {
                if (matchIdx >= currentIndex) {
                    matchCount++;
                }
            }
            
            // If majority achieved, send commit message to followers
            if (matchCount > getKnownNodes().size() / 2) {
                System.out.println("[DEBUG]->handleAppendEntriesReply gonna send commit");
                System.out.println("[DEBUG] Achieved majority for index " + currentIndex + 
                ". Sending commit message.");
                commitIndex = currentIndex;
                System.out.println("Commit index for index: " + commitIndex);
                // Send commit notification to followers
                Message commitMsg = new Message(
                    OPERATION.COMMIT_INDEX,
                    commitIndex,
                    getNodeName(),
                    getNodeId(),
                    getGossipNode().getHeartbeatService().getUDPport()
                );
                this.getGossipNode().getHeartbeatService().broadcast(commitMsg, true);
            }
        } else if(!reply.isSuccess() && reply.getlastLogIndex()!= null){
            System.out.println("[DEBUG]->handleAppendEntriesReply-> unsucessfull and getlastLogIndex isnt null");
            Integer index= reply.getlastLogIndex();
            System.out.println("the lastlog index is: " + reply.getlastLogIndex());
            switch (index){
                case -1:
                default:
            }
        } else {
            // If append failed, decrement nextIndex and retry
            System.out.println("[DEBUG]->handleAppendEntriesReply-> unsucessfull");
            System.out.println("[DEBUG]->handleAppendEntriesReply->  - nextIndex: "  + nextIndex.get(reply.getnodeID()));
            System.out.println("[DEBUG]->handleAppendEntriesReply->  - matchIndex: " + matchIndex.get(reply.getnodeID()));
            decrementNextIndex(reply.getnodeID());
            

        }
    }
    */


   /**
    * Applies all committed entries to the state machine.
    * Processes log entries with indexes greater than lastApplied up to the commitIndex.
    *
    * @see Nodes.Raft.RaftNode#processLogEntry(LogEntry)
    */
    private void applyCommittedEntries() {
        // Apply all newly committed entries to state machine
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = log.get(lastApplied);
            processLogEntry(entry);
        }
    }

   /**
    * Processes a single log entry by applying the corresponding operation to the document.
    * Parses the log entry command, extracts the operation and document, and applies the operation.
    *
    * @param entry The log entry to process.
    * @see Node#processOP(OPERATION, Document)
    */
    private void processLogEntry(LogEntry entry) {    
        try {
            addNewLog("REPLICATION", entry);
            String[] parts = entry.getCommand().split(":", 2);
            if (parts.length != 2) return;
            OPERATION op = OPERATION.valueOf(parts[0]);
            Document doc = Document.fromString(parts[1]);
            System.out.println("[DEBUG] Applying log entry: Operation=" + op + 
                          ", Document=" + doc + ", Term=" + entry.getTerm() +
                          ", Index=" + entry.getIndex());
            System.out.println("[DEBUG]: GOING TO PROCESS DOCUMENT OP WITH SUPER;");
            super.processOP(op, doc); // Process the document operation
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid operation in log entry: " + entry.getCommand());
            e.printStackTrace();
        }
    }

   /**
    * Handles a commit index message from the leader.
    * Updates the local commit index and applies newly committed entries to the state machine.
    *
    * @param message The commit index message.
    * @see Nodes.Raft.RaftNode#applyCommittedEntries()
    */
    public synchronized void handleCommitIndex(Message message) {
        System.out.println("[DEBUG]->handleCommitIndex");
        try {
            int leaderCommitIndex = (Integer) message.getPayload();
            System.out.println("[DEBUG] Received commit index: " + leaderCommitIndex + 
                          ", current commit index: " + commitIndex);
            // Update local commit index (take minimum of leader's commit index and our last log index)
            commitIndex = Math.min(leaderCommitIndex, log.size() - 1);
            System.out.println("[DEBUG] Updated commit index to: " + commitIndex + 
                          ", applying entries...");
            
            // Apply any newly committed entries
            applyCommittedEntries();
            
        } catch (Exception e) {
            LOGGER.severe("Error handling commit index: " + e.getMessage());
            e.printStackTrace();
        }
    }

   /**
    * Replicates the latest log entry to all peers using multicast.
    * Constructs an AppendEntriesArgs object with the latest log entry and its preceding entry's information.
    * Broadcasts the AppendEntriesArgs to all peers for log replication.
    *
    * @see Nodes.Raft.AppendEntriesArgs
    * @see Services.HeartbeatService#broadcast(Message, boolean)
    */
    private void replicateLogMULTICAST() {
        System.out.println("[DEBUG]->REPLICATING LOG");
        // Instead of unicasting to each peer, broadcast the latest entry
        if (!log.isEmpty()) {
            LogEntry latestEntry = log.get(log.size() - 1);
            // The entry before our latest entry
            int prevLogIndex = log.size() - 2;
            int prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).getTerm() : 0;
            /*
             If log is empty (size 0): prevLogIndex = -1, prevLogTerm = 0
             If log has one entry (size 1): prevLogIndex = -1, prevLogTerm = 0
             If log has multiple entries: prevLogIndex = second-to-last index, prevLogTerm = term of that entry
             */
            AppendEntriesArgs args = new AppendEntriesArgs(
                currentTerm.get(),
                getNodeId(),
                prevLogIndex, // Index of the entry that should come before our new entry
                prevLogTerm,  // Term of that previous entry
                Collections.singletonList(latestEntry), // NEW ENTRY WE WANT TO ADD
                commitIndex
            );
    
            Message appendMsg = new Message(
                OPERATION.APPEND_ENTRIES,
                args,
                getNodeName(),
                getNodeId(),
                getGossipNode().getHeartbeatService().getUDPport()
            );
    
            this.getGossipNode().getHeartbeatService().broadcast(appendMsg, true);
        }
    }

    // private void replicateLogUNICAST() {
    //     System.out.println("[DEBUG]->REPLICATING LOG");
    //     for (Map.Entry<UUID, Integer> peer : getKnownNodes()) {
    //         System.out.println("[DEBUG]->REPLICATING LOG--for");
    //         UUID peerId = peer.getKey();
    //         if (!peerId.equals(getNodeId())) {
    //             System.out.println("[DEBUG]->REPLICATING LOG--if");
    //             // int nextIdx = nextIndex.getOrDefault(peerId, log.size()-1);
    //             // List<LogEntry> entries = log.subList(nextIdx, log.size());
                
    //             System.out.println("[DEBUG]->For peer " + peerId + ":");
    //             System.out.println("[DEBUG]->nextIdx: " + nextIdx);
    //             System.out.println("[DEBUG]->entries size: " + entries.size());


    //             if (!entries.isEmpty()) {
    //                 System.out.println("[DEBUG]->Sending " + entries.size() + " entries to peer: " + peerId);
    //                 sendAppendEntries(peerId, nextIdx - 1, entries);
    //             }else {
    //                 System.out.println("[DEBUG]->No entries to send - nextIdx: " + nextIdx + ", log size: " + log.size());
    //             }
    //         }else {
    //             System.out.println("[DEBUG]->Skipping self");
    //         }
    //     }
    // }

   /**
    * Handles a sync request from another node.
    * Updates the logs and documents based on the received sync payload.
    *
    * @param payload The sync request payload.
    * @return The updated size of the log after processing the sync request.
    * 
    * @see Nodes.Raft.RaftNode#updateLogsFromSync(String)
    * @see Nodes.Raft.RaftNode#processDocumentsFromSync(String)
    */
    public int handleSyncRequest(String payload) {
        System.out.println("[DEBUGGING] handleSyncRequest");
        System.out.println("[DEBUGGING] handleSyncRequest->paylaod->" +payload);
        String[] parts = payload.split(";");
        
        //int senderTerm = Integer.parseInt(parts[2]);
        // Step down if we see a higher term
        // if (senderTerm > currentTerm.get()) {
        //     //stepDown(senderTerm);
        //     return;
        // }

        // Process logs
        String logsSection = parts[3].substring(5); // Skip "LOGS:" prefix
        System.out.println("[DEBUGGING] handleSyncRequest gooing to updateLogsFromSync ");
        System.out.println("[DEBUGGING] handleSyncRequest logsSection-> "+logsSection);
        updateLogsFromSync(logsSection);
        System.out.println("[DEBUGGING] handleSyncRequest back from updateLogsFromSync ");
        // Process documents
        String docsSection = parts[4]; // Skip "DOCS:" prefix
        System.out.println("[DEBUGGING] handleSyncRequest gooing to processDocumentsFromSync ");
        System.out.println("[DEBUGGING] handleSyncRequest docsSection-> "+docsSection);
        processDocumentsFromSync(docsSection);
        System.out.println("[DEBUGGING] handleSyncRequest back from processDocumentsFromSync logsSize()-> "+ log.size());
        // Send acknowledgment
        //this.getGossipNode().getHeartbeatService().sendSyncAck(senderId, senderPort);
        return log.size();
    }

   /**
    * Updates the logs based on the received sync payload.
    *
    * @param logsSection The logs section of the sync payload.
    *
    * @see Nodes.Raft.RaftNode#appendLogEntry(LogEntry)
    */
    private void updateLogsFromSync(String logsSection) {
        System.out.println("[DEBUGGING] updateLogsFromSync-> logsize() -> " + log.size());
        System.out.println("[DEBUGGING] updateLogsFromSync-> logslogsSectionze() -> " + logsSection);
        // Parse and update logs
        String[] logEntries = logsSection.split("#");
        System.out.println("[DEBUGGING] updateLogsFromSync-> logEntries() -> " +logEntries);
        for (String logEntry : logEntries) {
            System.out.println("[DEBUGGING] updateLogsFromSync-> logEntry() -> " +logEntry);
            // Parse log entry and add if newer
            LogEntry entry = LogEntry.fromString(logEntry);
            System.out.println("[DEBUGGING] updateLogsFromSync-> entry.getIndex()() -> " + entry.getIndex() + "logsize() " + log.size());
            System.out.println("[DEBUGGING] updateLogsFromSync-> entry " + entry );

            if (entry.getIndex() >= log.size()) {
                System.out.println("[DEBUGGING] updateLogsFromSync");
                appendLogEntry(entry);
            }
        }
        System.out.println("[DEBUGGING]  after updateLogsFromSync. LOGS: " + log.toString());
    }

   /**
    * Processes the documents from the sync payload.
    *
    * @param docsSection The documents section of the sync payload.
    *
    * @see Nodes.DocumentsDB#updateOrAddDocument(Document)
    */
    private void processDocumentsFromSync(String docsSection) {
        String[] docs = docsSection.split("\\$");
        for (String doc : docs) {
            if (!doc.isEmpty()) {
                System.out.println("[DEBUGGING]  processDocumentsFromSync. doc: " + doc);
                Document document = Document.fromString(doc);
                getDocuments().updateOrAddDocument(document);
            }
        }
    }

    /**
    * Waits for log replication to reach a majority of nodes.
    *
    * @param index The index to wait for replication.
    * @return true if replication is successful, false if it times out.
    * @see Nodes.Raft.RaftNode#applyCommittedEntries()
    */
    private boolean waitForLogReplication(int index) {
        System.out.println("[DEBUG]: INSIDE waitForLogReplication");
        System.out.println("[DEBUG]: INDEX: " + index);
        long startTime = System.currentTimeMillis();
        int requiredReplicas = (getKnownNodes().size() / 2) + 1;
        
        while (System.currentTimeMillis() - startTime < REPLICATION_TIMEOUT) {
            int replicationCount = 1; // Count self
            
            // Count nodes that have replicated this index
            for (Map.Entry<UUID, Integer> entry : matchIndex.entrySet()) {
                if (entry.getValue() >= index) {
                    System.out.println("Votes/ACKs received for node: " + entry.getValue()+" with index: "+index );

                    replicationCount++;
                }
            }
            
            // Check if we have majority
            if (replicationCount >= requiredReplicas) {
                commitIndex = index;
                applyCommittedEntries();
                return true;
            }
            
            try {
                Thread.sleep(100); // Small delay before next check
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        System.out.println("Log replication timed out for index: " + index);
        return false;
    }


    /**
     * Updates the indices tracking log replication progress for a follower.
     * Called when a follower successfully acknowledges AppendEntries.
     *
     * @param followerId The UUID of the follower node
     * 
     * @see Nodes.Raft.RaftNode#updateCommitIndex()
     */
    public synchronized void updateFollowerIndices(UUID followerId) throws NullPointerException{
        // Get the last index sent to this follower
        Integer currentNextIndex  = nextIndex.get(followerId);
        if(currentNextIndex == null){
            throw new NullPointerException("There was no entry in the map for the following node: " + followerId);
        }
        System.out.println("[DEBUG]->updateFollowerIndices-> Updating indices for " + followerId);
        System.out.println("[DEBUG]->updateFollowerIndices-> currentNextIndex: " + currentNextIndex );
        System.out.println("[DEBUG]->updateFollowerIndices-> log.size(): " + log.size());
        System.out.println("[DEBUG]->updateFollowerIndices-> log.content(): " + log.toString());
        System.out.println("[DEBUG]->updateFollowerIndices-> matchIndex: " + matchIndex);

        // Update matchIndex since we know the follower has matched up to this point
        matchIndex.put(followerId, currentNextIndex );
        // Update nextIndex for future sends
        nextIndex.put(followerId, currentNextIndex+1 );
        
       
        System.out.println("AFTER updating followers indices");
        System.out.println("[DEBUG]->updateFollowerIndices-> nextIndex: " + nextIndex.get(followerId));
        System.out.println("[DEBUG]->updateFollowerIndices-> matchIndex: " + matchIndex.get(followerId));
        
        // Check if we can advance the commit index
        updateCommitIndex();
    }

    /**
     * Decrements the nextIndex for a follower after a failed AppendEntries.
     * This helps find the point of divergence in the logs.
     *
     * @param followerId The UUID of the follower node
     * 
     * @see Nodes.Raft.RaftNode#sendAppendEntries()
     */ 
    public void decrementNextIndex(UUID followerId) {
        int currentNext = nextIndex.getOrDefault(followerId, log.size());
        System.out.println("[DEBVUG]->decrementNextIndex-> current nextIndex: " + currentNext);
        if (currentNext > 1) {  // Don't decrement below 1
            nextIndex.put(followerId, currentNext - 1);
            
            // Trigger a new AppendEntries with the decremented index
            int prevIndex = currentNext - 2;
            System.out.println("[DEBUG]->decrementNextIndex-> trigeering a new [AppendEntries] from index: " + prevIndex+1 +" to " + log.size());
            List<LogEntry> entries = log.subList(prevIndex + 1, log.size());
            sendAppendEntries(followerId, prevIndex, entries);
        }
    }

    /**
     * Updates the commit index if a majority of followers have replicated entries.
     * This is called after successful AppendEntries replies.
     * 
     * @see Nodes.Raft.RaftNode#applyCommittedEntries()
     */
    private void updateCommitIndex() {
        // Sort matched indices to find the median (majority)
        List<Integer> matchedIndices = new ArrayList<>(matchIndex.values());
        Collections.sort(matchedIndices);
        System.out.println("[DEBUG]updateCommitIndex-> matchIndex list: " + matchedIndices.toString()  );
        System.out.println("[DEBUG]updateCommitIndex-> matchIndex list size: " + matchedIndices.size()  );
        System.out.println("[DEBUG]updateCommitIndex-> matchIndex list size/2: " + matchedIndices.size()/2  );
        //System.out.println("[DEBUG]updateCommitIndex-> matchIndex list size/2: " + matchedIndices.size()/2  );
        // Get the index that has been replicated to a majority of nodes
        int majorityIndex = matchedIndices.get(matchedIndices.size() / 2);
        System.out.println("[DEBUG]updateCommitIndex-> majorityIndex: " + majorityIndex+ "; commitIndex: " + commitIndex +
                              "; majorityIndexes term:  " +log.get(majorityIndex).getTerm() + "; current term: " + currentTerm.get() );
        // Only update commit index:
        // 1. If the majority index is greater than our current commit index
        // 2. If the entry at majority index is from our current term
        if (majorityIndex > commitIndex && 
            log.get(majorityIndex).getTerm() == currentTerm.get()) {
            
            commitIndex = majorityIndex;
            applyCommittedEntries();
        }
    }

    /*
                                ██████  ██    ██ ███████ ██████  ██████  ██ ██████  ██ ███    ██  ██████  
                                ██    ██ ██    ██ ██      ██   ██ ██   ██ ██ ██   ██ ██ ████   ██ ██       
                                ██    ██ ██    ██ █████   ██████  ██████  ██ ██   ██ ██ ██ ██  ██ ██   ███ 
                                ██    ██  ██  ██  ██      ██   ██ ██   ██ ██ ██   ██ ██ ██  ██ ██ ██    ██ 
                                ██████    ████   ███████ ██   ██ ██   ██ ██ ██████  ██ ██   ████  ██████  
                                                                                                                                       
     */
    
    // @Override
    // protected synchronized void processOP(OPERATION op, Document document) {
    //     if (state.get() != NodeState.LEADER) {
    //         // Forward to leader if we're not the leader would be implemented in case all nodes could receive an operation order from a client.
    //         return;
    //     }
    //     // Don't directly process - create log entry first
    //     LogEntry entry = new LogEntry(
    //         currentTerm.get(),
    //         log.size(),
    //         String.format("%s:%s", op.toString(), document.toString())
    //     );
    //     System.out.println("[DEBUGGING] appendLogEntry processOP");
    //     appendLogEntry(entry);
        

    //     // Replicate to followers
    //     //replicateLog();

    //     // Only process after majority confirmation
    //     if (waitForLogReplication(log.size() - 1)) {
    //         super.processOP(op, document);
    //     }
    // }

    /**
     * Processes and commits messages when the node is the leader.
     * 
     * @see Nodes.Raft.RaftNode#appendLogEntry()
     * @see Nodes.Raft.RaftNode#replicateLogMULTICAST()
     * @see Nodes.Raft.RaftNode#processMessage()
     * @see Nodes.Raft.RaftNode#getLeaderQueue()
     */
    @Override
    public void processAndCommit() {
        System.out.println("PROCESS AND COMMIT RAFTNODE");
        if (!isLeader()) {
            return;
        }
        // if (state.get() == NodeState.LEADER) { //already checked before the call of the method
        //     super.processAndCommit();
        // }
        try {
            MessageQueue queue = getLeaderQueue();
            System.out.println("[DEBUG] Got leader queue: " + (queue != null));
            if (queue != null) {
                System.out.println("[DEBUG] Queue is empty: " + queue.isEmpty());
            }
            if (queue != null && !queue.isEmpty()) {
                System.out.println("[DEBUG] TOstring queue: " + queue.toString());
                Message message = queue.dequeue();
                System.out.println("[DEBUG] Dequeued message: " + message);
                if (message != null) {
                    System.out.println("[DEBUG] Processing message: " + message.getOperation() + 
                             " for document: " + message.getPayload());
                    // Create log entry
                    LogEntry entry = new LogEntry(
                        currentTerm.get(),
                        log.size(),
                        String.format("%s:%s", message.getOperation().toString(), 
                                    message.getPayload().toString())
                    );
                    System.out.println("[DEBUG]->Before adding entry - log size: " + log.size());
                    System.out.println("[DEBUG]->Before adding entry - log: " + log.toString());

                    System.out.println("Adding Logging entry from message queue");
                    appendLogEntry(entry);
                    System.out.println("[DEBUG]->After adding entry - log size: " + log.size());
                    
                    // for (UUID peerId : nextIndex.keySet()) {
                    //     if (!peerId.equals(getNodeId())) {
                    //         nextIndex.put(peerId, 0);
                    //     }
                    // }

                    // Actively replicate to followers
                    //replicateLogUNICAST();
                    replicateLogMULTICAST();
                    // Replicate and process
                    if (waitForLogReplication(log.size() - 1)) {
                        System.out.println("[DEBUG] Waiting for replication of index: " + (log.size() - 1));
                        processMessage(message);
                        System.out.println("[DEBUG] Finished processing message");
                    } else {
                        System.out.println("[DEBUG] Failed to replicate message");
                    }
                }
            }
        } catch (RemoteException e) {
            LOGGER.severe("Error processing message queue: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /*
███████ ██    ██ ██      ██          ███████ ██    ██ ███    ██  ██████ 
██      ██    ██ ██      ██          ██       ██  ██  ████   ██ ██      
█████   ██    ██ ██      ██          ███████   ████   ██ ██  ██ ██      
██      ██    ██ ██      ██               ██    ██    ██  ██ ██ ██      
██       ██████  ███████ ███████     ███████    ██    ██   ████  ██████ 
                                                                       
 */

    /**
    * Starts the full sync process to create an updated DB in a new node.
    * Using RAFT Protocol schema (using logs).
    *
    * @return The message containing the full sync content.
    * @see shared.Message 
    */
    @Override
    public Message startFullSyncProcess() {
        String operationID = UniqueIdGenerator.generateOperationId(OPERATION.FULL_SYNC_ANS.hashCode() + 
                                                                 Long.toString(System.currentTimeMillis()));
        
        StringBuilder payloadBuilder = new StringBuilder()
            .append(operationID).append(";")
            .append(getNodeId()).append(":")
            .append(this.getGossipNode().getHeartbeatService().getUDPport()).append(";")
            .append(currentTerm.get()).append(";"); // Include current term

        // Add log entries
        payloadBuilder.append("LOGS:").append(getLogsAsString()).append(";");

        // Add documents
        this.getDocuments().getDocuments().values().forEach(doc -> {
            payloadBuilder.append(doc.toString()).append("$");
        });

        return new Message(OPERATION.FULL_SYNC_ANS, payloadBuilder.toString());
    }

    /**
    * Adds a new node to the nextIndex and matchIndex maps.
    *
    * @param newNodeID The UUID of the new node.
    * @param index The initial index value for the new node.
    */
    public void addNewNodeToMaps(UUID newNodeID, int index ){
        System.out.println("[DEBUG]:addNewNodeToMaps: " +newNodeID+":"+index);
        nextIndex.putIfAbsent(newNodeID,index );
        matchIndex.putIfAbsent(newNodeID,index-1);

        System.out.println("[DEBUG]:addNewNodeToMaps: Nextindex:" +nextIndex.get(newNodeID)+" Matchindex: " +matchIndex.get(newNodeID) );
    }


/*
███████  █████  ██ ██      ██    ██ ██████  ███████ 
██      ██   ██ ██ ██      ██    ██ ██   ██ ██      
█████   ███████ ██ ██      ██    ██ ██████  █████   
██      ██   ██ ██ ██      ██    ██ ██   ██ ██      
██      ██   ██ ██ ███████  ██████  ██   ██ ███████ 


 */

    /**
    * Handles the failure of the leader node.
    * If the current node is a follower, it clears the leader state and starts a new election.
    *
    * @see Nodes.Raft.RaftNode#startElectionAfterFailure
    */
    public void handleLeaderFailure() {
        // Only handle if we're a follower
        if (state.get() == NodeState.FOLLOWER) {
            System.out.println("[DEBUG] Handling leader failure in node: " + getNodeName());
            
            // Clear leader state
            leaderId = null;
            
            // Start election process
            startElectionAfterFailure();
        }
    }

    /**
    * Simulates a sudden crash of the current node if it is the leader.
    * Stops all node services and threads.
    */
    public synchronized void simulateCrash() {
        if(getNodeState() != NodeState.LEADER) { return;}
        System.out.println("[DEBUG] Simulating sudden crash of node: " + getNodeName());

        // Force immediate termination of all node threads
        try {
            // Stop the heartbeat service
            if (getGossipNode() != null && getGossipNode().getHeartbeatService() != null) {
                // getGossipNode().getHeartbeatService().interrupt();
                // Thread heartbeatThread = getGossipNode().getHeartbeatService();
                // if (heartbeatThread != null) {
                //     heartbeatThread.stop(); // Force stop the heartbeat thread
                // }
                getGossipNode().getHeartbeatService().shutdown();
                
            }

            // Stop RMI services if they exist  // Not necessary to excecute? seems to be working both ways.
            // if (messageQueue != null) {
            //     messageQueue.unreg();
            // }

            // Stop scheduler threads
            if (scheduler != null) {
                scheduler.shutdownNow();

            }

            // Stop election monitor and heartbeat timer
            if (electionMonitor != null) {
                electionMonitor.cancel(false);
            }
            if (heartbeatTimer != null) {
                heartbeatTimer.cancel(false);
            }

            // Set running flag to false
            setRunning(false);
            // Finally, stop the main node thread
            //Thread.currentThread().stop();
        } catch (Exception e) {
            // In a real crash, we wouldn't even catch exceptions
            // But for testing purposes...
            System.err.println("Error during crash simulation: " + e.getMessage());
        }

    }

   /**
    * Starts an expedited election process after failure detection.
    * Resets the election timeout with a shorter duration, updates the election state,
    * and immediately broadcasts vote requests.
    *
    * @see Nodes.Raft.RaftNode#resetElectionTimeout(boolean)
    * @see Nodes.Raft.RaftNode#sendVoteRequest()
    */
    private void startElectionAfterFailure() {

        System.out.println("[DEBUG] Starting expedited election after failure detection");
        // Reset timeout with shorter duration
        resetElectionTimeout(true);
        
        // Reset election state
        currentTerm.incrementAndGet();
        state.set(NodeState.CANDIDATE);
        votedFor.set(getNodeId());
        votesReceived.clear();
        votesReceived.add(getNodeId());
        
        // Broadcast vote requests immediately
        sendVoteRequest();
        
    }

   /**
    * Simulates putting the node to sleep.
    * Sets the isSleeping flag to true and suspends the heartbeat service and message processing.
    *
    * @see Services.HeartbeatService#HBsuspend()
    */
    public void simulateSleep() {
        System.out.println("[DEBUG] Putting node " + getNodeName() + " to sleep");
        this.isSleeping = true;
        
        // Suspend heartbeat service and message processing
        if (getGossipNode() != null && getGossipNode().getHeartbeatService() != null) {
            getGossipNode().getHeartbeatService().HBsuspend();
        }
    }

    /**
    * Simulates waking up the node.
    * Sets the isSleeping flag to false, resumes the heartbeat service and message processing,
    * and resets the election timeout.
    *
    * @see Nodes.Raft.RaftNode#resetElectionTimeout()
    * @see Services.HeartbeatService#HBresume()
    */
    public void simulateWakeup() {
        System.out.println("[DEBUG] Waking up node " + getNodeName());
        this.isSleeping = false;
        
        // Resume heartbeat service and message processing
        if (getGossipNode() != null && getGossipNode().getHeartbeatService() != null) {
            getGossipNode().getHeartbeatService().HBresume();
        }
        
        // Reset election timeout
        resetElectionTimeout();
    }
    




    
}

