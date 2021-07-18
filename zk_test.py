#!/usr/bin/python3
import sys
import zmq
import kazoo
import netifaces
from kazoo.client import KazooClient
#Centralized broker class for the Pub/Sub system. Should act as a middleware that connects publishers and subscribers based on defined 'topics'.

class Broker:
    #constructor
    def __init__(self):
        #zkserver = '127.0.0.1'
        zkserver = '10.0.0.1'
        #self.option = option
        self.path = "/broker"
        self.election_path = "/broker_elect"
        self.ip = netifaces.ifaddresses(netifaces.interfaces()[1])[netifaces.AF_INET][0]['addr']
        self.zk = self.start_zkclient(zkserver)
        self.publishers = {}
        #zmq context
        self.context= zmq.Context()
        #socket registering publishers and subscribers to the broker. acts as a 'server' in client-server architecture
        self.register_socket = self.context.socket(zmq.REP)
        #publishes information to subscribers
        self.publishing_socket = self.context.socket(zmq.PUB)
        #receives information from publishers
        self.subscribing_socket = self.context.socket(zmq.REP)
        self.poller = zmq.Poller()

    def start(self):
        print(f"Broker: {self.ip}")
        print(self.zk.get_children(self.election_path))
        if not self.zk.exists(self.election_path):
            self.zk.create(self.election_path)

        #print("Awaiting for Election Results")
        election = self.zk.Election(self.election_path, self.ip)
        print("Contendors for election")
        print(election.contenders())
        election.run(self.leader_elected)
    
    def leader_elected(self):

        @self.zk.DataWatch(self.path)
        def broker_watch(data, stat):
            print("Elected broker")
            print(f"Broker: watcher triggered. data:{data}, stat={stat}")
            if data is None:
                if not self.zk.exists(self.path):
                    self.zk.create(self.path, value=self.ip.encode(
                        'utf-8'), ephemeral=True)
        
        print("Starting broker with option 1...")
        #take requests for registering with pubsub system on 5555
        self.register_socket.bind("tcp://*:5555")
        self.subscribing_socket.bind("tcp://*:5556")
        self.publishing_socket.bind("tcp://*:5557")

        #register with poller
        self.poller.register(self.register_socket, zmq.POLLIN)
        self.poller.register(self.subscribing_socket, zmq.POLLIN)
        print("Starting event loop with option 1...")
        while True:
            events = dict(self.poller.poll())

            #if (self.register_socket in events):
                #self.recv_register1()
            #if self.subscribing_socket in events:
                #self.recv_subscribing1()

    def start_zkclient(self, zkserver):
        port = '2181'
        url = f'{zkserver}:{port}'
        print(f"starting ZK client on '{url}'")
        zk = KazooClient(hosts=url)
        zk.start()
        return zk
    

def main():
    test_broker = Broker()
    test_broker.start()

main()