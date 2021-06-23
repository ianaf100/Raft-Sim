package iaf29.a2.raft.server;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorKilledException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Kill;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import iaf29.a2.Client;
import iaf29.a2.raft.log.LogEntry;
import iaf29.a2.raft.server.messages.*;
import iaf29.a2.raft.statemachine.Command;
import iaf29.a2.raft.statemachine.StateMachine;


public class RaftServer extends AbstractActor {

    public final static boolean RANDOM_FAILURES = true;
    public final static boolean ADD_LATENCY = true;
    public final static int TIMEOUT_VARIANCE = 150;
    public final static int TIMEOUT_MIN = 150;
    public final static int HEARTBEAT_INTERVAL = 60;

    StateModule state;
    StateMachine stateMachine;
    LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    boolean restarted = false;
    Random random;
    Cancellable currentTimeout;
    Map<ActorRef, Cancellable> heartbeats;
    Cancellable candidateHeartbeat;
    ActorRef clientRef;

    Receive followerBehavior = receiveBuilder()
        .match(WriteRequest.class, req -> {
            clientRef = getSender();
            getSender().tell(new Client.RejectRequest(state.lastLeader, req.serialNumber), getSelf());
        })
        .match(FollowerTimeout.class, t -> handleTimeout())
        .match(AppendEntriesRPC.class, rpc -> handleLeaderRPC(rpc))
        .match(RequestVoteRPC.class, rpc -> vote(rpc))
        .match(RequestVoteResponse.class, ignore -> {})
        .match(ErrorCommand.class, e -> handleErrorCommand(e))
        .match(AppendEntriesResponse.class, ignore -> {})
        .match(HeartbeatTick.class, ignore -> {})
        .matchAny(msg -> log.debug("Follower ignoring message " + msg))
        .build();

    Receive candidateBehavior = receiveBuilder()
        .match(WriteRequest.class, req -> {
            clientRef = getSender();
            getSender().tell(new Client.RejectRequest(state.lastLeader, req.serialNumber), getSelf());
        })
        .match(FollowerTimeout.class, t -> handleTimeout())
        .match(HeartbeatTick.class, h -> sendVoteRequests())
        .match(RequestVoteResponse.class, res -> handleVoteResponse(res))
        .match(RequestVoteRPC.class, rpc -> vote(rpc))
        .match(AppendEntriesRPC.class, rpc -> handleLeaderRPC(rpc))
        .match(ErrorCommand.class, e -> handleErrorCommand(e))
        .match(AppendEntriesResponse.class, ignore -> {})
        .matchAny(msg -> log.debug("Candidate ignoring message " + msg))
        .build();

    Receive leaderBehavior = receiveBuilder()
        .match(
            HeartbeatTick.class, 
            h -> h.receiver != null,
            h -> handleLeaderHeartbeat(h))
        .match(WriteRequest.class, req -> handleRequest(req))
        .match(AppendEntriesResponse.class, res -> handleFollowerResponse(res))
        .match(RequestVoteRPC.class, rpc -> vote(rpc))
        .match(AppendEntriesRPC.class, rpc -> handleLeaderRPC(rpc))
        .match(ErrorCommand.class, e -> handleErrorCommand(e))
        .match(RequestVoteResponse.class, ignore -> {})
        .match(FollowerTimeout.class, ignore -> {})
        .matchAny(msg -> log.debug("Leader ignoring message " + msg))
        .build();


    public RaftServer() {
        this.random = new Random();
    }

    RaftServer(StateModule state) {   // for testing
        this();
        this.state = state;
    }

    static Props props(StateModule state) {
        return Props.create(RaftServer.class, () -> new RaftServer(state));
    }

    @Override
    // Post-Crash Pre-Restart - cancel timers and forward setup config to replacement
    public void preRestart(Throwable reason, Optional<Object> message) {
        cancelHeartbeats();
        if (currentTimeout != null) 
            currentTimeout.cancel();
        getSelf().tell(new Setup(state.otherServers), ActorRef.noSender());
    }

    @Override
    public void postRestart(Throwable reason) {
        restarted = true;
        if (reason instanceof ActorKilledException) { // deliberate kill, simulate delay before restarting
            sleep(500);   
        } else if (reason instanceof ServerStalledException) { // deliberate stall
            log.debug("Stalling");
            sleep(1000*10000);
        }
        log.info("RESTARTING\n");
    }

    private void addLatency() {
        // int r = random.nextInt(4);
        // if (r > 0) {
        //     sleep(random.nextInt(10));
        // } else {
        //     sleep(random.nextInt(20));
        // }
        int r = random.nextInt(30);
        if (r < 15) {  // small latency
            sleep(random.nextInt(10));
        } else if (r < 25) {   // medium latency
            sleep(random.nextInt(20));
        } else {   // big latency
            sleep(random.nextInt(30));
        }
    }

    private void sleep(int ms) {
        try {
            Thread.sleep(ms);  
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handleRequest(WriteRequest req) throws IOException {
        clientRef = getSender();
        getSender().tell(new Client.AcknowledgeRequest(req.serialNumber), getSelf());
        
        // handles duplicate writes if leader crashes after committing, before responding
        if (req.serialNumber == stateMachine.lastSerialNumber()) {
            getSender().tell(new Client.Response(req.serialNumber, stateMachine.lastResponse()), getSelf());
            return;

        } else if (req.command != null) {   // empty request = initial connection, no response
            log.info("Received client request: " + req.command.toString());
            if (req.serialNumber != state.localLog.lastSerial()) {
                LogEntry newEntry = new LogEntry(req.command, state.currentTerm);
                state.localLog.appendEntry(newEntry);
                log.info(state.localLog.toString());
                state.flushToDisk();
            }
            for (ActorRef server : state.otherServers) {
                if (!state.activeRequests.containsKey(server)) {   // hold off for active RPCs
                    beginHeartbeat(server);  
                    AppendEntriesRPC rpc = buildAppendEntriesRPC(server);
                    if (ADD_LATENCY) addLatency();
                    server.tell(rpc, ActorRef.noSender());
                    state.activeRequests.put(server, rpc);
                }
            }
        }
    }

    private AppendEntriesRPC buildAppendEntriesRPC(ActorRef server) {
        if (state.nextIndex == null) {
            log.info("nextIndex is null");
        }

        int nextIndex = state.getNextIndex(server);
        int prevLogIndex = nextIndex - 1;
        int prevLogTerm;
        LogEntry[] entries;

        if (state.localLog.size() == 0 || prevLogIndex == 0) {
            prevLogTerm = 0;
        } else {
            prevLogTerm = state.localLog.getEntry(prevLogIndex).term();
        }
        if (state.localLog.lastIndex() < nextIndex) {  // for heartbeats, check if entries need sending
            entries = new LogEntry[0];
        } else {
            entries = new LogEntry[]{state.localLog.getEntry(nextIndex)};  // supports 1 entry at a time (for now)
        }

        log.info("send rpc{" +
            state.getActorName(server) + ",term=" +
            state.currentTerm + ",prevIndex=" +
            prevLogIndex + ",prevTerm=" +
            prevLogTerm + ",commit=" +
            state.commitIndex + "}"
        );

        return new AppendEntriesRPC(
            state.currentTerm, 
            getSelf(), 
            prevLogIndex, 
            prevLogTerm,
            entries,
            state.commitIndex,
            ++state.nextRequestId);
    }

    private void handleFollowerResponse(AppendEntriesResponse response) throws IOException {
        if (RANDOM_FAILURES) {
            if (random.nextInt(100) == 0) {
                getSelf().tell(Kill.getInstance(), ActorRef.noSender());
            }
        }
        // log.info("received response " + state.getActorName(getSender()) + " term " + response.term);
        if (response.term > state.currentTerm) {
            state.currentTerm = response.term;
            log.info("Stepping down as leader");
            becomeFollower();  
            return;
        }
        AppendEntriesRPC active = state.activeRequests.get(getSender());
        if (active != null && response.requestId == active.requestId) {
            if (response.success) {
                int matchIndex = active.prevLogIndex + active.entries.length;
                state.setMatchIndex(getSender(), matchIndex);
                state.setNextIndex(getSender(), matchIndex + 1);
                state.activeRequests.remove(getSender());
                if (state.checkCommitIndex(matchIndex)) {
                    log.info("Advancing commitIndex as leader to " + matchIndex);
                    while (state.lastApplied < state.commitIndex) {
                        state.lastApplied += 1;
                        Command command = state.localLog.getEntry(state.lastApplied).command();
                        int result = stateMachine.applyInput(command);
                        if (ADD_LATENCY) addLatency();
                        clientRef.tell(new Client.Response(command.serialNumber, result), getSelf());
                    }
                }
                // check if more messages are needed before next heartbeat/request
                if (state.localLog.lastIndex() >= state.getNextIndex(getSender())) {
                    beginHeartbeat(getSender());
                    AppendEntriesRPC rpc = buildAppendEntriesRPC(getSender());
                    state.activeRequests.put(getSender(), rpc);
                    if (ADD_LATENCY) addLatency();
                    getSender().tell(rpc, ActorRef.noSender());
                }
            // failed response
            } else {
                state.setNextIndex(getSender(), state.getNextIndex(getSender()) - 1);
                beginHeartbeat(getSender());
                AppendEntriesRPC rpc = buildAppendEntriesRPC(getSender());
                state.activeRequests.put(getSender(), rpc);
                if (ADD_LATENCY) addLatency();
                getSender().tell(rpc, ActorRef.noSender());
            }
        }
    }

    private void handleLeaderHeartbeat(HeartbeatTick h) {
        AppendEntriesRPC rpc = buildAppendEntriesRPC(h.receiver);
        state.activeRequests.put(h.receiver, rpc);
        if (ADD_LATENCY) addLatency();
        h.receiver.tell(rpc, ActorRef.noSender());
    }

    private void handleLeaderRPC(AppendEntriesRPC rpc) throws IOException {
        int prevLogTerm, nextIndex;
        if (rpc.term < state.currentTerm) {
            state.flushToDisk();
            if (ADD_LATENCY) addLatency();
            rpc.leaderId.tell(new AppendEntriesResponse(state.currentTerm, false, rpc.requestId), getSelf());
            log.debug("replying false, my term is higher");
        } else {
            beginTimer();
            if (rpc.term > state.currentTerm) {
                becomeFollower();  // or reset if already a follower
                state.currentTerm = rpc.term;
                state.lastLeader = rpc.leaderId;
                log.debug("Acknowledge new leader " + state.lastLeader.path());
            }

            // no entry at prevLogIndex
            if (state.localLog.lastIndex() < rpc.prevLogIndex) {
                state.flushToDisk();
                if (ADD_LATENCY) addLatency();
                rpc.leaderId.tell(new AppendEntriesResponse(state.currentTerm, false, rpc.requestId), getSelf());
                log.debug("replying false, no entry exists");
                return;
            }

            if (state.localLog.size() == 0 || rpc.prevLogIndex == 0) {  
                prevLogTerm = 0;
            } else {
                prevLogTerm = state.localLog.getEntry(rpc.prevLogIndex).term();
            }

            // unmatching entry at prevLogIndex
            if (prevLogTerm != rpc.prevLogTerm) {
                state.flushToDisk();
                if (ADD_LATENCY) addLatency();
                rpc.leaderId.tell(new AppendEntriesResponse(state.currentTerm, false, rpc.requestId), getSelf());
                log.debug("replying false, mismatched entry");
                return;
            }

            // update log entries
            nextIndex = rpc.prevLogIndex + 1;
            for (int i = 0; i < rpc.entries.length; i++) {
                int currentIndex = nextIndex + i;
                if (currentIndex > state.localLog.size()) {
                    state.localLog.appendEntry(rpc.entries[i]);
                    log.info("Updated Log: " + state.localLog.toString());
                } else {
                    LogEntry currentEntry = state.localLog.getEntry(currentIndex);
                    if (currentEntry.term() != rpc.entries[i].term()) {
                        state.localLog.removeTail(currentIndex);
                        state.localLog.appendEntry(rpc.entries[i]);
                        log.info("Updated Log: " + state.localLog.toString());
                    }

                }
            }
            if (rpc.leaderCommit > state.commitIndex) {
                state.commitIndex = Math.min(rpc.leaderCommit, state.localLog.lastIndex());
                updateStateMachine();
            }
            state.flushToDisk();
            if (ADD_LATENCY) addLatency();
            rpc.leaderId.tell(new AppendEntriesResponse(state.currentTerm, true, rpc.requestId), getSelf());
            log.debug("replying true, commit index " + state.commitIndex);
        }
    }

    private void sendVoteRequests() {
        log.debug("Sending vote requests");
        for (ActorRef receiver : state.otherServers) {
            if (!state.yesVotes.contains(receiver) && !state.noVotes.contains(receiver)) {
                RequestVoteRPC rpc = new RequestVoteRPC(state.currentTerm, getSelf(), 
                        state.localLog.lastIndex(), state.localLog.lastTerm());
                if (ADD_LATENCY) addLatency();
                receiver.tell(rpc, ActorRef.noSender());
            }
        }
    }

    private void handleVoteResponse(RequestVoteResponse response) {
        log.debug("Received [" + response.voteGranted + "," + response.term + "] from " + state.getActorName(getSender()));
        if (response.term > state.currentTerm) {  
            state.currentTerm = response.term;
            becomeFollower();
        } else {
            if (response.voteGranted == false) {
                state.noVotes.add(getSender());
            } else {
                state.yesVotes.add(getSender());
                int majority = ((state.otherServers.size() + 1) / 2) + 1;
                if (state.yesVotes.size() >= majority) {
                    becomeLeader();
                    log.info("I'm leader");
                }
            }
        }
    }

    private void becomeFollower() {
        getContext().become(followerBehavior);
        beginTimer();
        cancelHeartbeats();
        state.clearLeaderState();
    }

    private void becomeLeader() {
        getContext().become(leaderBehavior); 
        currentTimeout.cancel();
        candidateHeartbeat.cancel();
        candidateHeartbeat = null;
        state.setLeaderState();
        for (ActorRef server : state.otherServers) {
            beginHeartbeat(server);
        }
    }

     // become candidate and start an election
     private void handleTimeout() {
        state.currentTerm++;
        getContext().become(candidateBehavior);
        log.info("Timed out, term " + state.currentTerm);
        beginTimer();
        cancelHeartbeats();
        state.yesVotes.clear();
        state.noVotes.clear();
        state.yesVotes.add(getSelf());
        state.votedFor = getSelf();
        candidateHeartbeat = getContext().system()
            .scheduler()
            .scheduleWithFixedDelay(Duration.ZERO, Duration.ofMillis(HEARTBEAT_INTERVAL), getSelf(), 
                new HeartbeatTick(null), getContext().system().dispatcher(), ActorRef.noSender()
            );
    }

    public void beginHeartbeat(ActorRef receiver) {
        if (heartbeats.get(receiver) != null) {
            heartbeats.get(receiver).cancel();
            heartbeats.remove(receiver);
        }
        ActorSystem system = getContext().system();
        heartbeats.put(receiver, system
            .scheduler()
            .scheduleWithFixedDelay(Duration.ZERO, Duration.ofMillis(HEARTBEAT_INTERVAL), getSelf(), 
                new HeartbeatTick(receiver), system.dispatcher(), ActorRef.noSender())
            );
    }

    private void cancelHeartbeats() {
        if (candidateHeartbeat != null) {
            candidateHeartbeat.cancel();
            candidateHeartbeat = null;
        }
        for (ActorRef server : state.otherServers) {
            if (heartbeats.get(server) != null) {
                heartbeats.get(server).cancel();
                heartbeats.remove(server);
            }
        }
    }

    private void vote(RequestVoteRPC rpc) {
        log.debug("vote request [" + state.getActorName(rpc.candidateId) + ",term:" + rpc.term + "], my current term is " + state.currentTerm);
        boolean grantVote = false;
        if (rpc.term > state.currentTerm) {
            becomeFollower();  // change to follower or reset if already a follower
            state.currentTerm = rpc.term;
            grantVote = true;
        }
        if (rpc.term == state.currentTerm) {
            if (state.votedFor == null || state.votedFor == rpc.candidateId) {
                if (rpc.lastLogTerm > state.localLog.lastTerm()) {
                    grantVote = true;
                }
                if (rpc.lastLogTerm == state.localLog.lastTerm() && rpc.lastLogIndex >= state.localLog.lastIndex()) {
                    grantVote = true;
                }
            }
        }
        if (grantVote) {
            state.votedFor = rpc.candidateId;
            beginTimer();
            log.debug("Voting for " + state.getActorName(rpc.candidateId));
        }
        if (ADD_LATENCY) addLatency();
        rpc.candidateId.tell(new RequestVoteResponse(state.currentTerm, grantVote), getSelf());
    }

    public void beginTimer() {
        if (currentTimeout != null) {
            currentTimeout.cancel();
        }
        ActorSystem system = getContext().system();
        int timeoutInterval = random.nextInt(TIMEOUT_VARIANCE) + TIMEOUT_MIN;
        // log.debug("Timeout=" + timeoutInterval);
        currentTimeout = system
            .scheduler()
            .scheduleOnce(Duration.ofMillis(timeoutInterval), getSelf(), new FollowerTimeout(), system.dispatcher(), ActorRef.noSender());
    }

    private void updateStateMachine() throws IOException {
        while (state.lastApplied < state.commitIndex) {
            state.lastApplied += 1;
            log.debug("Committing " + state.lastApplied + " to state machine");
            Command command = state.localLog.getEntry(state.lastApplied).command();
            stateMachine.applyInput(command);
        }
    }

    // receive an error command for debugging
    private void handleErrorCommand(ErrorCommand e) {
        if (e.terminate) {
		    getContext().stop(getSelf());
        } else {
            throw e.exception;   // throw provided exception which determines what to do on restart
        } 
    }

    // initialize all state variables, loading from disk if restart
    private void handleSetup(Setup s) throws IOException {
        if (!s.testing) {  // test mode, don't change state
            state = new StateModule(getSelf(), s.servers, restarted);
            if (restarted) {
                log.info("Restored log: " + state.localLog.toString() + ", term: " + state.currentTerm);
            }
        }
        stateMachine = new StateMachine(state.id());
        heartbeats = new HashMap<ActorRef, Cancellable>();
    }

    @Override
	// initial receive behavior on startup: wait for setup msg then become a follower
	public Receive createReceive() {
        return receiveBuilder()
            .match(Setup.class, s -> {
                handleSetup(s);
                if (!restarted)
                    state.currentTerm++;  // advance from term 0 to 1
                becomeFollower();
            })
            .matchAny(ignore -> {})
            .build();
    }
}
