package iaf29.a2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import iaf29.a2.raft.RaftSystem;


public class Main {

    public static final int DEFAULT_COUNT = 5;

    public static void main(String[] args) {

        int serverCount = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_COUNT;

        ActorSystem system = ActorSystem.create("raft-system");

        ActorRef mainSupervisor = system.actorOf(RaftSystem.props(serverCount), "main-supervisor");
        mainSupervisor.tell(new RaftSystem.Setup(), ActorRef.noSender());
        ActorRef client = system.actorOf(Client.props(mainSupervisor), "client");
        client.tell(new Client.FindLeader(), ActorRef.noSender()); 

        LoggingAdapter log = Logging.getLogger(system, system);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            String command = null;
            try {
                command = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            log.info("RECEIVED DEBUG COMMAND: " + command);
            mainSupervisor.tell(new RaftSystem.DebugCommand(command), ActorRef.noSender());
        }
    }
    
}
