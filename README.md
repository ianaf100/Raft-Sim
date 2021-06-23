## Raft Implementation in Akka

Raft implementation attempt using [Akka](https://akka.io/) in Java,
SE647 Assignment 2 by Ian Franklin

An implementation of the [Raft distributed consensus algorithm](https://raft.github.io/), with simulated client interaction and server failure events. Running the application will show log output of 5 distributed machines using the algorithm to agree on log order given simulated user write requests and given simulated server crashes.

### Usage
Built using Gradle. To run the simulation with the default 5 servers, execute the following inside `a2/` :

```
gradle run
```

### Config

Update `loglevel` in `src/main/resources/application.conf` to see debug logging.

#### Creating Failures

Different types of failures can be simulated, either as user input or through randomized events outlined in the next section. To execute a failure command on a server, enter `command n` during execution with n=0-4 for the following commands:

- `restart`: kills and restarts a server with a 500ms delay
- `stop`: calls `SupervisorStrategy.stop()`, stopping the server permanently
- `stall`: server goes to sleep() for 10000 seconds
- `poison`: sends a `PoisonPill`, making the server stop and disappear forever

#### Constants

These are constants that change how the simulation behaves: (note that any restart adds a 500ms delay)

- `Client.RANDOM_REQUESTS`: The client continuously generates random requests to send to Raft, happening at a random interval between 0ms and `MAX_REQUEST_INTERVAL`
- `RaftServer.RANDOM_FAILURES`: Leaders randomly fail and restart (1/100 chance every response)
- `RaftServer.ADD_LATENCY`: All message sends are preceded with 0-20ms random latency (more likely to be under 10ms)
  - Latency values and frequencies can be changed within `RaftServer.addLatency()`
- `RaftSystem.KILL_AT_RANDOM`: A random server is killed and restarted every 1.5-4 seconds

