# CS 6381 Project 2

In this project, we extended our Topic-based Pub/Sub broker from project 1 to implement fault tolerance by leader election. Whether we use broker-based (option 1) or direct (option 2) dissemination , the Publishers and Subscribers in the system will continue to be able to write and read to their respective topics, as long as at least one Broker middleware is still running.

Instructions - how to run/test our workflow:

(Note: if the user is unfamiliar/unable to run mininet or zookeeper on their machine, please ping Akshar or Ishan for access to our Chameleon Cloud server. Our workflow was easiest to test using this setup. For all mininet commands, please include the '&' sign at the end so that they run in the background, or you may have to restart the workflow from scratch.)

  1) Spin up a 'mininet' instance. This is the easiest way to test that our system works across multipls hosts. In this example, we will spin up 16 hosts with this command: 
    mn --switch ovs --controller ref --topo tree,depth=2,fanout=4
  2) Start zookeeper client on host 1 (h1) (our code for the broker as is hardcodes that zookeeper will always be running on 10.0.0.1 for ease of use on our system, although this could be easily configured by passing a command line argument). Example miniet command: 
    h1 /usr/share/zookeeper/bin/zkServer.sh start&
    
    (Note: If zookeeper is already running, please run 'h1 /usr/share/zookeeper/bin/zkCli.sh rmr /broker' and 'h1 /usr/share/zookeeper/bin/zkServer.sh stop' before starting the zookeeper server. Having a lingering setup from before can interfere with how a new deployment of our system acts.)
    
  3) Start some brokers, with the dissemination option of your choice. The first one you start will automatically be elected leader, while all others will wait on their turn. Since we will be disconnecting (or 'killing') the leader broker to test fault tolerance, it is advised to spin up multiple brokers before running the kill commad, so that a new leader is elected.
   Example: h2 python3 broker.py 1&
   This would start a broker on host 2 (h2) (10.0.0.2) with dissemination option 1 (broker-based).
   You could start a direct dissemination broker by running 'h2 python3 broker.py 2&' instead. Our system was not designed to run with 2 different kinds of brokers at the same time, as it was a requirement to have global configuration of the option (eg - publishers and subscribers will configure themselves to respond properly to only one type of broker) so please spin up all of the same type in one run of this workflow.
   
  4) Start some publishers/subscribers. Syntax is 'h3 python3 publisher.py <zookeeper ip> <topic>&', and 'h4 python3 subscriber.py <zookeeper ip> <topic>&'. You may spin up as many of each as you'd like, and try out different topics to see that the right messages are being sent. At the moment, we have publishers just send automated messages based on the topic name to avoid manually punching in values for testing.
  
  5) View output on publisher/subscriber/broker hosts. If a publisher/subscriber/broker is currently running on h3, running 'h3 ps' in mininet will show you the console output of the program since the last time that you ran the command (or since you started the python program). Subscribers, publishers, and brokers will all output different messages to the console to describe what is happening on the internals of the system -  For example, registering to a topic, receiving/sending data, connecting to a new broker, etc.
  
  6) Kill off the leader broker. Running 'h2 ps' if the leader is currently running on a broker will give you some output in this format:
    <some console output if applicable>
        PID TTY          TIME CMD
       3058 pts/6    00:00:09 python3
       7733 pts/6    00:00:00 ps
       24806 pts/6    00:00:00 bash
      
     You will want to grab the pid of the process running python3, and kill that one. Continuing the example with broker running on h2-
      h2 kill -9 3058
     Since 3058 was the pid of the process running python.
      
  7) View the output of different hosts using the 'hx ps' command from step 5 to see how publishers, subscribers, and brokers react to the leader dying. Sometimes, due to zookeeper delay and/or connection loss, it may take 30s-1min for these hosts to react. If so, just keep running the ps command on the host intermittently until some kind of output shows up.
      
      One final thing to note is that it is possible for some of the clients to fail to connect to zookeeper, which will result in an error that looks like this:
      
  Connection dropped: socket connection broken
Transition to CONNECTING
Traceback (most recent call last):
  File "broker.py", line 258, in <module>
    main()
  File "broker.py", line 256, in main
    test_broker.start()
  File "broker.py", line 51, in start
    if not self.zk.exists(self.election_path):
  File "/usr/local/lib/python3.6/dist-packages/kazoo/client.py", line 1123, in exists
    return self.exists_async(path, watch=watch).get()
  File "/usr/local/lib/python3.6/dist-packages/kazoo/handlers/utils.py", line 69, in get
    raise self._exception
kazoo.exceptions.ConnectionLoss

  In this situation, just try rerunning the same command again after a few seconds, it usually is okay. If Zookeeper continues to act funky, we suggest restarting it using the cli, like specified in step 1. 
      
 Thank you, and let us know if you have any questions about this process!
      
      
Latency Testing and Plots:
```
      The latency plots for the end to end time for a transaction as there is no change in the configuration of the broker, publisher or subscriber.
```
