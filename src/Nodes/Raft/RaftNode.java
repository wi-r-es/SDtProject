package Nodes.Raft;

import java.lang.System.Logger;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import shared.Message;
import shared.OPERATION;
import Nodes.Node;

import java.util.*;

public class RaftNode extends Node {
    //FOR LOGS
    //private static final Logger LOGGER = Logger.getLogger(RaftNode.class.getName());
    private List<LogEntry> log;
    //CONSTANTS
    private static final int ELECTION_TIMEOUT_MIN = 150;  
    private static final int ELECTION_TIMEOUT_MAX = 300;  
    private static final int HEARTBEAT_INTERVAL = 50;     


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

    public NodeState getNodeState() {
        return state.get();
    }

    public int getCurrentTerm() {
        return currentTerm.get();
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

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

    @Override
    protected boolean checkQueue() throws RemoteException {
        // Your queue checking logic that's Raft-aware
        if (state.get() == NodeState.LEADER) {
            return super.checkQueue();
        }
        return false;
    }
    @Override
    protected void processAndCommit() {
        // Your processing logic that's Raft-aware
        if (state.get() == NodeState.LEADER) {
            super.processAndCommit();
        }
    }
    public synchronized String getLogsAsString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Log Entries for Node ").append(getNodeName()).append(":\n");
        
        new ArrayList<>(log).forEach(entry -> 
            sb.append(String.format("Term: %d, Index: %d,Comand: %s%n", 
                entry.getTerm(), entry.getIndex(), entry.getCommand())));
                
        return sb.toString();
    }
    @Override
    protected void printKnownNodes() {
        super.printKnownNodes();
        System.out.println("Current Raft[" +getNodeName()+ "]State: " + state.get());
        System.out.println("Current Term: " + currentTerm.get());
        
        getLogsAsString();
    }
    @Override
    public boolean isLeader(){
        if(state.get() == NodeState.LEADER)
        {return true;}
        else return false;
    }

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
    

    public void scheduleElectionTimeout() {
        Random random = new Random();
        String parts[] = this.getNodeName().split("-");
        int id = Integer.parseInt(parts[1]);
        int electionTimeout =  random.nextInt(150) + 200 + (id % 50);
        ScheduledExecutorService executorService= Executors.newScheduledThreadPool(1);;
        executorService.schedule(this::startElection, electionTimeout, TimeUnit.MILLISECONDS);
    }

    
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


    
    private void sendVoteRequest() { // in a perfect world should send this via unicast to every other node instead of multicasting
        Message voteRequest = new Message(
            OPERATION.VOTE_REQ,
            new RequestVoteArgs(currentTerm.get(), getNodeId(), log.size() - 1, getLastLogTerm())
        );
        System.out.println(voteRequest);
        System.out.println("[DEBUG] Sending vote request to peers." );
        this.getGossipNode().getHeartbeatService().broadcast(voteRequest, false);
        
    }

    private int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    private boolean isLogUpToDate(int lastLogIndex, int lastLogTerm) {
        int myLastLogTerm = getLastLogTerm();
        return (lastLogTerm > myLastLogTerm) ||
                (lastLogTerm == myLastLogTerm && lastLogIndex >= log.size() - 1);
    }

    public synchronized void handleVoteRequest(RequestVoteArgs args) {
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
            getPeerPort(args.getCandidateId())
        );
    }

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

    

    private void becomeLeader() {
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
    }
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

    @SuppressWarnings("deprecation")
    private void broadcastHeartbeat() {
        
        Message heartbeat = Message.LheartbeatMessage(
            "Heartbeat from leader:" + getNodeId() + ":" + currentTerm
        );
        this.getGossipNode().getHeartbeatService().broadcast(heartbeat, false);
    }

    

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

    public synchronized void handleAppendEntries(AppendEntriesArgs args) {
        // Implementation for AppendEntries RPC/UDP (leader to followers)
    }
    
}

