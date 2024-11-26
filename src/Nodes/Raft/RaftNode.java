package Nodes.Raft;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import shared.Message;
import shared.OPERATION;
import Nodes.Node;

import java.util.*;

public class RaftNode extends Node {
    private int currentTerm;
    private UUID votedFor;
    private List<LogEntry> log;
    private NodeState role;
    private UUID leaderId;
    private Set<UUID> votesReceived;
    private Timer electionTimer;

    public RaftNode(String nodeId, boolean isLeader) throws RemoteException {
        super(nodeId, isLeader);
        this.currentTerm = 0;
        this.votedFor = null;
        this.log = new ArrayList<>();
        this.role = NodeState.FOLLOWER;
        this.leaderId = null;
        this.votesReceived = ConcurrentHashMap.newKeySet();
        this.electionTimer = new Timer(true);

        if (isLeader) {
            becomeLeader();
        } else {
            startElectionTimeout();
        }
    }

    public void resetElectionTimer() {
        electionTimer.cancel();
        startElectionTimeout();
    }

    private synchronized void becomeLeader() {
        role = NodeState.LEADER;
        leaderId = getNodeId();
        System.out.println("[DEBUG] Node " + getNodeName() + " became leader for term " + currentTerm);
        // Start periodic heartbeats to all followers.
        startHeartbeats();
    }

    public void scheduleElectionTimeout() {
        Random random = new Random();
        String parts[] = this.getNodeName().split("-");
        int id = Integer.parseInt(parts[1]);
        int electionTimeout =  random.nextInt(150) + 200 + (id % 50);
        ScheduledExecutorService executorService= Executors.newScheduledThreadPool(1);;
        executorService.schedule(this::startElection, electionTimeout, TimeUnit.MILLISECONDS);
}

    private void startElectionTimeout() {
        if (electionTimer != null) {
            electionTimer.cancel(); // Cancel the existing timer
        }
        electionTimer = new Timer(true); // Create a new timer

        long timeout = randomElectionTimeout(); 
        electionTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (role == NodeState.FOLLOWER) {
                    startElection();
                }
            }
        }, timeout);
        System.out.println("[DEBUG] Node " + getNodeName() + " set election timeout to " + timeout + " ms");
    }

    private long randomElectionTimeout() {
        return 1500 + new Random().nextInt(3000); // 150-300 ms range. -> changed to double// kept changing cause it aint working
    }

    public synchronized void startElection() {
        currentTerm++;
        role = NodeState.CANDIDATE;
        votedFor = getNodeId();
        votesReceived.clear();
        votesReceived.add(getNodeId());

        System.out.println(getNodeName() + " started election for term " + currentTerm);


        System.out.println("[DEBUG] Node " + getNodeName() + " is requesting votes for term " + currentTerm);
        // Send vote requests to all peers.
        // for (Map.Entry<UUID, Integer> peer : getKnownNodes()) {
        //     sendVoteRequest(peer.getKey());
        // }
        sendVoteRequest();

        startElectionTimeout(); // Restart election timeout.
    }

    // private void sendVoteRequest(UUID peerId) {
    //     Message voteRequest = new Message(
    //         OPERATION.VOTE_REQ,
    //         new RequestVoteArgs(currentTerm, getNodeId(), log.size() - 1, getLastLogTerm())
    //     );
    //     System.out.println("[DEBUG] Sending vote request to peer: " + peerId);
    //     this.getGossipNode().getHeartbeatService().sendUncompMessage(voteRequest, peerId, getPeerPort(peerId) );
    // }
    private void sendVoteRequest() {
        Message voteRequest = new Message(
            OPERATION.VOTE_REQ,
            new RequestVoteArgs(currentTerm, getNodeId(), log.size() - 1, getLastLogTerm())
        );
        System.out.println(voteRequest);
        System.out.println("[DEBUG] Sending vote request to peers." );
        this.getGossipNode().getHeartbeatService().broadcast(voteRequest, false);
        
    }

    private int getLastLogTerm() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    public synchronized void handleVoteRequest(RequestVoteArgs args) {
        System.out.println("[DEBUG] Hadnling Vote request." );
        System.out.println(args);
        if (args.getTerm() > currentTerm) {
            currentTerm = args.getTerm();
            role = NodeState.FOLLOWER;
            votedFor = null;
            //resetElectionTimer();
        }
        //int lastLogTerm = 
        boolean voteGranted = false;
        // boolean logIsUpToDate = args.getLastLogTerm() > lastLogTerm || 
        //                 (args.getLastLogTerm() == lastLogTerm && args.getLastLogIndex() >= lastLogIndex);

        if(args.getTerm() == currentTerm &&
                (votedFor == null || votedFor.equals(args.getCandidateId())) &&
                isLogUpToDate(args.getLastLogIndex(), args.getLastLogTerm())){
                    votedFor = args.getCandidateId();
                    voteGranted = true;
                    resetElectionTimer(); // Reset timeout since we acknowledge a leader
        }

        // if (voteGranted) {
        //     votedFor = args.getCandidateId();
        // }

        Message voteResponse = new Message(
            OPERATION.VOTE_ACK,
            new RequestVoteReply(currentTerm, voteGranted, this.getNodeId())
        );
        //printKnownNodes();
        this.getGossipNode().getHeartbeatService().sendUncompMessage( voteResponse, args.getCandidateId(), getPeerPort(args.getCandidateId()) );
    }

    private boolean isLogUpToDate(int lastLogIndex, int lastLogTerm) {
        int myLastLogTerm = getLastLogTerm();
        return (lastLogTerm > myLastLogTerm) ||
                (lastLogTerm == myLastLogTerm && lastLogIndex >= log.size() - 1);
    }

    public synchronized void handleVoteResponse(RequestVoteReply reply) {
        System.out.println("[DEBUG] [node: "+ this.getNodeName()+ "]Received vote response from " + reply.getVoterId() +
                       " for term " + reply.getTerm() + ": vote granted = " + reply.isVoteGranted());
        if (role != NodeState.CANDIDATE || reply.getTerm() != currentTerm) return;

        if (reply.isVoteGranted()) {
            votesReceived.add(reply.getVoterId());
            System.out.println("known nodes: " + this.getKnownNodes().size());
            if (votesReceived.size() > (getKnownNodes().size() / 2)) {
                becomeLeader();
            }
        } else if (reply.getTerm() > currentTerm) {
            currentTerm = reply.getTerm();
            role = NodeState.FOLLOWER;
            votedFor = null;
            resetElectionTimer();
        }

         // Check for split vote
        if (role == NodeState.CANDIDATE && votesReceived.size() <= (getKnownNodes().size() / 2)) {
            retryElection(); // Retry election
        }
    }
    private void retryElection() {
        if (role == NodeState.CANDIDATE) {
            System.out.println("[DEBUG] Node " + getNodeName() + " restarting election for term " + (currentTerm + 1));
            try {
                Thread.sleep(randomElectionTimeout()); 
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            startElection();
        }
    }

    private void startHeartbeats() {
        Timer heartbeatTimer = new Timer(true);
        heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (role == NodeState.LEADER) {
                    broadcastHeartbeat();
                }
            }
        }, 0, 150); // Send heartbeats every 50ms.
    }

    private void broadcastHeartbeat() {
        Message heartbeat = Message.LheartbeatMessage(
            "Heartbeat from leader: " + getNodeId() + " term: " + currentTerm
        );
        this.getGossipNode().getHeartbeatService().broadcast(heartbeat, false);
    }

    public synchronized void handleAppendEntries(AppendEntriesArgs args) {
        // Implementation for AppendEntries RPC (leader to followers)
    }

    public synchronized void handleHeartbeat(Message message) {
        Object payload = message.getPayload();
        if (payload instanceof String) {
            String[] parts = ((String) payload).split(":");
            UUID leaderNodeId = UUID.fromString(parts[1]);
            if(leaderNodeId.equals(this.getNodeId())){return;}
            int leaderTerm = Integer.parseInt(parts[2]);
    
            System.out.println("[DEBUG] Received heartbeat from leader " + leaderNodeId +
                               " for term " + leaderTerm);
    
            if (leaderTerm >= currentTerm) {
                currentTerm = leaderTerm;
                leaderId = leaderNodeId;
                role = NodeState.FOLLOWER;
                resetElectionTimer();
            }
        }
    }
    
}

