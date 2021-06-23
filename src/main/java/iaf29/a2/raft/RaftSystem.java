package iaf29.a2.raft;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorKilledException;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.OneForOneStrategy;
import akka.actor.PoisonPill;
import akka.actor.PreRestartException;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.DeciderBuilder;

import iaf29.a2.raft.server.RaftServer;
import iaf29.a2.raft.server.ServerStalledException;
import iaf29.a2.raft.server.ServerStoppedException;
import iaf29.a2.raft.server.messages.ErrorCommand;


// RaftSystem is responsible for setting up & supervising the raft servers, routing clients to random raft 
//  servers, and executing debugging commands such as killing, restarting servers

public class RaftSystem extends AbstractActor {

    public static final boolean KILL_AT_RANDOM = true;

    int serverCount;
    List<ActorRef> servers;
    Random random;
    LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(int serverCount) {
        return Props.create(RaftSystem.class, () -> new RaftSystem(serverCount));
    }

    /* Messages */
    public static class Setup {
    }

    public static class RequestRandomServer {
        public final ActorRef current;

        public RequestRandomServer(ActorRef current) {
            this.current = current;
        }
    }

    public static class RandomServer {
        public final ActorRef server;

        public RandomServer(ActorRef server) {
            this.server = server;
        }
    }

    public static class DebugCommand {
        public final String command;

        public DebugCommand(String command) {
            this.command = command;
        }
    }

    public static class RandomFailure {
        public final int targetIndex;

        public RandomFailure(int targetIndex) {
            this.targetIndex = targetIndex;
        }
    }

    RaftSystem(int serverCount) {
        this.serverCount = serverCount;
        this.random = new Random();
        this.servers = new ArrayList<ActorRef>();
    }

    private static SupervisorStrategy strategy =
        new OneForOneStrategy(false, DeciderBuilder
                .match(ActorKilledException.class, e -> SupervisorStrategy.restart())
                .match(ServerStalledException.class, e -> SupervisorStrategy.restart())
                .match(PreRestartException.class, e -> SupervisorStrategy.restart())
                .match(ServerStoppedException.class, e -> SupervisorStrategy.stop())
                .matchAny(o -> SupervisorStrategy.escalate())
                .build());

    @Override
    public SupervisorStrategy supervisorStrategy() {
        return strategy;
    }

    private void initialize() {
        for (int i = 0; i < serverCount; i++) {
            ActorRef newActor = getContext().actorOf(Props.create(RaftServer.class), "server-" + i);
            servers.add(newActor);
            getContext().watch(newActor);
        }

        // initialize each with a list of all the other servers
        for (int i = 0; i < servers.size(); i++) {
            servers.get(i).tell(new iaf29.a2.raft.server.messages.Setup(servers), ActorRef.noSender());
        }

        if (KILL_AT_RANDOM) {
            scheduleRandomFailure();
        }
    }

    private void sendRandomServer(RequestRandomServer r) {
        ActorRef server = servers.get(random.nextInt(serverCount));
        while (server == r.current) {
            server = servers.get(random.nextInt(serverCount));
        }
        getSender().tell(new RandomServer(server), ActorRef.noSender());
    }

    private void handleDebugCommand(DebugCommand d) {
        String command = d.command;
        int serverID = Integer.parseInt(command.split(" ")[1]);
        if (command.startsWith("restart")) {
            servers.get(serverID).tell(Kill.getInstance(), ActorRef.noSender());
        } else if (command.startsWith("stall")) {
            servers.get(serverID).tell(new ErrorCommand(new ServerStalledException("stalled")), ActorRef.noSender());
        } else if (command.startsWith("stop")) {
            servers.get(serverID).tell(new ErrorCommand(new ServerStoppedException("stopped")), ActorRef.noSender());
        } else if (command.startsWith("poison")) {
            servers.get(serverID).tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
    }

    private void scheduleRandomFailure() {
        ActorSystem system = getContext().system();
        int timeoutInterval = random.nextInt(4000) + 1500;
        int target = random.nextInt(5);
        system
            .scheduler()
            .scheduleOnce(Duration.ofMillis(timeoutInterval), getSelf(), new RandomFailure(target), system.dispatcher(), ActorRef.noSender());
    }

    private void randomFailure(RandomFailure r) {
        if (KILL_AT_RANDOM) {
            servers.get(r.targetIndex).tell(Kill.getInstance(), ActorRef.noSender());
            log.info("Randomly restarted server-" + r.targetIndex);
            scheduleRandomFailure();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Setup.class, s -> initialize())
            .match(RequestRandomServer.class, r -> sendRandomServer(r))
            .match(DebugCommand.class, d -> handleDebugCommand(d))
            .match(RandomFailure.class, r -> randomFailure(r))
            .match(akka.actor.Terminated.class, t -> log.info(t.actor() + " HAS TERMINATED"))
            .build();
    }

}
