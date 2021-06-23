package iaf29.a2;

import java.time.Duration;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import iaf29.a2.raft.server.messages.WriteRequest;
import iaf29.a2.raft.statemachine.Command;
import iaf29.a2.raft.RaftSystem;

// Represents a Client that can interact with a raft system, find the leader,
//  send random write requests, and resend indefinitely unreturned requests

public class Client extends AbstractActor {

    public final static boolean RANDOM_REQUESTS = true;
    public final static int MAX_REQUEST_INTERVAL = 200;
    public final static int TIMEOUT_INTERVAL = 400;

    public final ActorRef raftSystem;
    private ActorRef leader;
    private WriteRequest activeRequest;
    private Cancellable currentTimeout = null;
    Random random;
    LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    int currentSerial = 0;  

    public static Props props(ActorRef raftSystem) {
        return Props.create(Client.class, () -> new Client(raftSystem));
    }

    /* Messages */
    public static class FindLeader {
    }

    public static class RejectRequest{
        public final ActorRef expectedLeader;
        public final int serialNumber;

        public RejectRequest(ActorRef leader, int serialNumber) {
            expectedLeader = leader;
            this.serialNumber = serialNumber;
        }
    }
    
    public static class AcknowledgeRequest {
        public final int serialNumber;

        public AcknowledgeRequest(int serialNumber) {
            this.serialNumber = serialNumber;
        }
    }

    public static class Response {
        public final int serialNumber;
        public final int result;

        public Response(int serialNumber, int result) {
            this.serialNumber = serialNumber;
            this.result = result;
        }
    }

    public Client(ActorRef raftSystem) {
        this.raftSystem = raftSystem;
        this.activeRequest = null;
        this.leader = null;
        this.random = new Random();
    }

    // attempt to find new leader by requesting a random server from the raft system
    private void requestRandomServer(ActorRef currentServer) {
        if (leader != null) {
            log.info("Timed out");
        }
        leader = null;
        raftSystem.tell(new RaftSystem.RequestRandomServer(currentServer), getSelf());
    }

    // send initial request to a new Raft Server
    private void attemptConnection(ActorRef server) {
        // log.debug("Attempting connect to " + server.path());
        sendRequest(server, new WriteRequest(++currentSerial));
        scheduleTimeout(TIMEOUT_INTERVAL, new FindLeader());
    }

    // schedules a cancellable message to self after a delay in milliseconds
    private void scheduleTimeout(int interval, Object message) {
        if (currentTimeout != null) {
            currentTimeout.cancel();
        }
        ActorSystem system = getContext().system();
        currentTimeout = system
            .scheduler()
            .scheduleOnce(Duration.ofMillis(interval), getSelf(), message, system.dispatcher(), ActorRef.noSender());
    }

    // requested server is not the leader
    private void handleRejection(RejectRequest r) {
        if (r.serialNumber == currentSerial) {  // ignore stale responses
            currentTimeout.cancel();
            // log.debug(getSender().path() + " is not leader");
            if (r.expectedLeader == null || r.expectedLeader == getSender()) {
                requestRandomServer(getSender());
            } else {
                attemptConnection(r.expectedLeader);
            }
        }
    }

    // request has been acknowledged by leader.
    //  for a first connection, this confirms the leader has been found and requests can be sent
    private void handleAcknowledge(AcknowledgeRequest a) {
        // new leader found
        if (leader == null && a.serialNumber == currentSerial) {
            currentTimeout.cancel();
            log.debug("Found leader: " + getSender().path());
            leader = getSender();

            if (activeRequest == null && RANDOM_REQUESTS) {
                scheduleRandomRequest();
            } else {
               sendRequest(leader, activeRequest);  // resend incomplete request
            }
        }
    }

    private void handleResponse(Response r) {
        assert r.serialNumber == currentSerial;
        log.debug("Received response: " + r.result);
        currentTimeout.cancel();
        activeRequest = null;  // TODO: fix/handle multiple responses
        if (RANDOM_REQUESTS) {
            scheduleRandomRequest();
        }
    }

    // generate a random command and random interval, and schedule a request at that interval
    private void scheduleRandomRequest() {
        int waitInterval = random.nextInt(MAX_REQUEST_INTERVAL);
        int serial = ++currentSerial;
        Command command;
        switch(random.nextInt(3)) {
            case 0:
                command = new Command(serial, "x<-" + random.nextInt(100));
                break;
            case 1:
                command = new Command(serial, "y<-" + random.nextInt(100));
                break;
            default:
                command = new Command(serial, "z<-" + random.nextInt(100));
                break;
        }
        activeRequest = new WriteRequest(command);
        scheduleRequest(activeRequest, waitInterval);
    }

    private void sendRequest(ActorRef server, WriteRequest request) {
        server.tell(request, getSelf());
        scheduleTimeout(TIMEOUT_INTERVAL, new FindLeader());
    }

    private void scheduleRequest(WriteRequest request, int waitInterval) {
        ActorSystem system = getContext().system();
        // log.info("Wait interval = " + waitInterval);
        system.scheduler()
            .scheduleOnce(Duration.ofMillis(waitInterval), leader, request, system.dispatcher(), getSelf());

        scheduleTimeout(TIMEOUT_INTERVAL + waitInterval, new FindLeader());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(FindLeader.class, s -> requestRandomServer(leader))
            .match(RaftSystem.RandomServer.class, r -> attemptConnection(r.server))
            .match(RejectRequest.class, r -> handleRejection(r))
            .match(AcknowledgeRequest.class, a -> handleAcknowledge(a))
            .match(Response.class, r -> handleResponse(r))
            .build();
    }
}
