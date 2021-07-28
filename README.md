# CS 6381 Project 3

In this project, we extended our Topic-based fault-tolerant Pub/Sub broker from project 2 to implement history length and ownership strength quality of services for each topic.
In our implementation, publishers must call the register api with a history length, which is then saved in zookeeper. Publishers can register to multiple topics at a time, and will be assigned an ownership strength based on what order they registered to a topic (eg - first one = 1, second one -2, etc.)

Subscribers also must register to a topic with a history length. Subscribers will receive messages from the active publisher with the highest ownership strength that is publishing a history length greater than its own. For example, on Topic A: if pub1 has history length 5 and and ownership strength 1, pub2 has history length 10 and ownership strength 2, and sub1 has history length 7, sub1 will listen to pub2. However, if sub1 had a history length 4 (lower than both), it would listen to pub1, because its owneship strength is higher.

This Quality of Service works in both broker-based and direct dissemination. Note that we did not have time to implement load balancing for this project, but fault tolerance on the brokers still works.

Instructions - how to run/test our workflow:

*See readme for previous project (https://github.com/ishankaul1/cs6381_project2) for instructions on how to start up mininet and run/kill python programs on different hosts*

We created some unit tests to show that the quality of service works. The Python scripts publishertest1-4.py, and subscribertest1-2.py start up publishers and subscribers that each connect to one or multiple topics, all with different history lengths. Due to how we coded our tests, it is suggested to run zookeeper on host 1, and ensure all brokers are started with the same dissemination option.

Once all the pubs/subs are connected, we test the workflow by killing off publishers, and checking subscribers' outputs to see which publisher they connected to for pubs on each topic that died. Output is currently logged directly to terminal; we apologize if it is a little tough to read.

Latency tests/Graphs:
