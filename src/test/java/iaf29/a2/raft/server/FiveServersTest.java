package iaf29.a2.raft.server;

import akka.testkit.javadsl.TestKit;
import iaf29.a2.raft.server.messages.AppendEntriesRPC;
import iaf29.a2.raft.server.messages.FollowerTimeout;
import iaf29.a2.raft.server.messages.RequestVoteRPC;
import iaf29.a2.raft.server.messages.Setup;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.time.Duration;
import java.util.Arrays;


public class FiveServersTest {

    // forwards messages to two actors, an intended target and an observer
    static class Forwarder extends AbstractActor {
        final ActorRef target;  // a raft server
        final ActorRef observer;  // a testkit probe

        public static Props props(ActorRef target, ActorRef observer) {
            return Props.create(Forwarder.class, () -> new Forwarder(target, observer));
        }
    
        @SuppressWarnings("unused")
        public Forwarder(ActorRef target, ActorRef observer) {
            this.target = target;
            this.observer = observer;
        }
    
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .matchAny(message -> {
                    target.forward(message, getContext());
                    observer.forward(message, getContext());
                }).build();
        }
    }
    
    static ActorSystem system;
    ActorRef[] forwarders;
    int serial = 0;

    @BeforeClass
    public static void setup() {
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    // starts the system at an example snapshot of an akka run, raft servers are connected to forwarders
    //   which forward all messages to returned test probes
    TestKit[] setupSystem(StateModule[] initialStates) {
        forwarders = new ActorRef[5];
        ActorRef[] servers = new ActorRef[5];
        TestKit[] probes = new TestKit[5];
        
        system = ActorSystem.create();

        // create and start raft servers
        for (int i = 0; i < 5; i++) {
            ActorRef newServer = system.actorOf(RaftServer.props(initialStates[i]), "server-" + i);
            servers[i] = newServer;
        }
        // set up forwarders and connect to test probes
        for (int i = 0; i < 5; i++) {
            probes[i] = new TestKit(system);
            ActorRef newForwarder = system.actorOf(Forwarder.props(servers[i], probes[i].getRef()), "forwarder-" + i);
            forwarders[i] = newForwarder;
        }
        // set up raft servers with the forwarders, so raft messages are forwarded to probes
        for (int i = 0; i < 5; i++) {
            initialStates[i].self = servers[i];
            initialStates[i].otherServers = Arrays.asList(forwarders);
            initialStates[i].id = initialStates[i].getActorName(initialStates[i].self);

            servers[i].tell(new Setup(Arrays.asList(forwarders), true), ActorRef.noSender());   // give the forwarders to the raft servers
        }
        return probes;
    }

    // create snapshot states for all servers
    StateModule[] createSnapshotStates() {
        StateModule[] states = new StateModule[5];

        for (int i = 0; i < 5; i++) {
            StateModule state = new StateModule();
            state.currentTerm = 28;
            state.votedFor = null;
            try {
                state.restoreLog("src/test/resources/test_log_1");  // rebuild log from test file
            } catch (Exception e) {
            } finally {
                states[i] = state;
            }
        }
        return states;
    }

    String getName(ActorRef actor) {
        String[] split = actor.path().toString().split("/");
        return split[split.length - 1];
    }

    @Test
    public void testTemplate() {
        system = ActorSystem.create();
        TestKit[] probes = setupSystem(createSnapshotStates());
        forwarders[0].tell(new FollowerTimeout(), ActorRef.noSender());  // trick server-0 to become leader

        probes[1].ignoreMsg((Object msg) -> msg instanceof RequestVoteRPC);
        AppendEntriesRPC rpc = probes[1].expectMsgClass(Duration.ofSeconds(3), AppendEntriesRPC.class);
        Assert.assertEquals("server-0", getName(rpc.leaderId));
    }







}
