package iaf29.a2.raft.server;

import akka.testkit.javadsl.TestKit;
import iaf29.a2.raft.log.LogEntry;
import iaf29.a2.raft.server.messages.AppendEntriesRPC;
import iaf29.a2.raft.server.messages.AppendEntriesResponse;
import iaf29.a2.raft.server.messages.RequestVoteRPC;
import iaf29.a2.raft.server.messages.Setup;
import iaf29.a2.raft.statemachine.Command;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.time.Duration;
import java.util.ArrayList;


public class SingleServerTest {
    
    static ActorSystem system;
    int serial = 0;

    private LogEntry createLogEntry(String command, int term) {
        Command newCommand = new Command(++serial, command);
        return new LogEntry(newCommand, term);
    }

    @BeforeClass
    public static void setup() {
        // system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testFirstLogUpdate() {
        system = ActorSystem.create();

        new TestKit(system) {
        {
            ArrayList<ActorRef> otherServers = new ArrayList<>();
            otherServers.add(getRef());
            StateModule state = new StateModule();
            final ActorRef testActor = system.actorOf(RaftServer.props(state), "server-" + 1);
            testActor.tell(new Setup(otherServers), ActorRef.noSender());
            testActor.tell(new AppendEntriesRPC(2, getRef(), 0, 0, new LogEntry[0], 0, 1), ActorRef.noSender());
            testActor.tell(new AppendEntriesRPC(2, getRef(), 0, 0, new LogEntry[]{createLogEntry("x<-11", 2)}, 0, 1), ActorRef.noSender());

            AppendEntriesResponse rpc = expectMsgClass(Duration.ofSeconds(1), AppendEntriesResponse.class);
            Assert.assertEquals(true, rpc.success);
        }};
    }

    @Test
    public void testLeaderTimeout() {
        system = ActorSystem.create();

        new TestKit(system) {
        {
            ArrayList<ActorRef> otherServers = new ArrayList<>();
            otherServers.add(getRef());
            StateModule state = new StateModule();
            final ActorRef testActor = system.actorOf(RaftServer.props(state), "server-" + 1);
            testActor.tell(new Setup(otherServers), ActorRef.noSender());
            testActor.tell(new AppendEntriesRPC(2, getRef(), 0, 0, new LogEntry[0], 0, 1), ActorRef.noSender());

            expectMsgClass(AppendEntriesResponse.class);
            RequestVoteRPC rpc = expectMsgClass(Duration.ofSeconds(1), RequestVoteRPC.class);
            Assert.assertEquals(3, rpc.term);
        }};
    }

    @Test
    public void testServerTemplate() {
        system = ActorSystem.create();

        new TestKit(system) {
        {
            ArrayList<ActorRef> otherServers = new ArrayList<>();
            otherServers.add(getRef());
            StateModule state = new StateModule();
            /* Change state to reflect test situation */
            
            /**/
            final ActorRef testActor = system.actorOf(RaftServer.props(state), "server-" + 1);
            testActor.tell(new Setup(otherServers), ActorRef.noSender());

            /* Send test messages */
            testActor.tell(new AppendEntriesRPC(2, getRef(), 0, 0, new LogEntry[0], 0, 1), ActorRef.noSender());

            AppendEntriesResponse response = expectMsgClass(AppendEntriesResponse.class);
            Assert.assertEquals(2, response.term);
        }};

    }

    /*
    private void becomeFollower() {
        getContext().become(followerBehavior);
        beginTimer();
        state.current = State.FOLLOWER;
        state.votedFor = null;
        cancelHeartbeats();
        yesVotes.clear();
        noVotes.clear();
        state.nextIndex.clear();
        state.matchIndex.clear();
        state.activeRequests.clear();
    }
    */
}
