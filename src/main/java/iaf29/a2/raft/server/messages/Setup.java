package iaf29.a2.raft.server.messages;

import java.util.List;

import akka.actor.ActorRef;

public class Setup {

    public final List<ActorRef> servers;
    public final boolean testing;

    public Setup(List<ActorRef> servers) {
        this(servers, false);
    }

    public Setup(List<ActorRef> servers, boolean testing) {
        this.servers = servers;
        this.testing = testing;
    }

}
