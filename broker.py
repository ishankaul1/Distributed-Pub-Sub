#!/usr/bin/python3
import zmq
import time
import sys
import netifaces
from kazoo.client import KazooClient
#Centralized broker class for the Pub/Sub system. Should act as a middleware that connects publishers and subscribers based on defined 'topics'.

class Broker:
    #constructor
    def __init__(self, option):
        zkserver = '10.0.0.1'
        #zkserver = '127.0.0.1'
        self.option = option
        self.path = "/broker"
        self.election_path = "/broker_elect"
        self.ip = netifaces.ifaddresses(netifaces.interfaces()[1])[netifaces.AF_INET][0]['addr']
        self.zk = self.start_zkclient(zkserver)

        if (self.option == 1):
            #dict to match each topic with a list of assiciated publishers
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
        elif (self.option == 2):
            self.subscribers = {} #NO LONGER USED
            self.publishers = {}
            self.context = zmq.Context()
            self.register_socket = self.context.socket(zmq.REP)
            self.notification_socket = self.context.socket(zmq.PUB) #notifies subscribers of changes to the publishers registerd to their topic; filter by topic
        else:
            print("ERROR: Invalid option")
    
    def start_zkclient(self, zkserver):
        port = '2181'
        url = f'{zkserver}:{port}'
        print(f"starting ZK client on '{url}'")
        zk = KazooClient(hosts=url)
        zk.start()
        return zk

    def start(self):
        print(f"Broker: {self.ip}")
        if not self.zk.exists(self.election_path):
            self.zk.create(self.election_path)

        print(self.zk.get_children(self.election_path))
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
            if data is None and not self.zk.exists(self.path):
                print("Creating /broker znode, value={self.ip}")
                self.zk.create(self.path, value=self.ip.encode('utf-8'))
            else:
                print("Setting /broker znode to new value: {self.ip}") 
                self.zk.set(self.path, value=self.ip.encode('utf-8'))

            
            time.sleep(30)
            self.post_elect()
        #self.post_elect()
    #run the broker; should initialize all socket bindings and start event/request loop
    def post_elect(self):
        if(self.option == 1):
            print("Starting broker with option 1...")
            #take requests for registering with pubsub system on 5555
            self.register_socket.bind("tcp://*:5555")
            self.subscribing_socket.bind("tcp://*:5556")
            self.publishing_socket.bind("tcp://*:5557")

            #register with poller
            self.poller.register(self.register_socket, zmq.POLLIN)
            self.poller.register(self.subscribing_socket, zmq.POLLIN)
        
            self.event_loop1()
        elif (self.option == 2):
            print("Starting broker with option 2...")
            print("generating publisher data from zookeeper")
            try:
                print(self.zk.get_children(self.path))
                topics = self.zk.get_children(self.path)
                if len(topics) > 0:
                    for t in topics:
                        self.publishers[t] = self.zk.get_children(self.path+'/'+t)
            except:
                print("No children")
                pass
            print(self.publishers)
            #take requests for registering on 5555
            self.register_socket.bind("tcp://*:5555")
            self.notification_socket.bind("tcp://*:5557")

            self.event_loop2()
        else:
            print("ERROR: Cannot start broker with invalid option")

    def register_pub(self, topic, publisher_addr):
        if not self.zk.exists(self.path + '/' + topic):
            self.zk.ensure_path(self.path + '/' + topic)
        if not self.zk.exists(self.path + '/' + topic + '/' + publisher_addr):
            self.zk.ensure_path(self.path + '/' + topic + '/' + publisher_addr)

        if (topic not in self.publishers):
            self.publishers[topic] = [publisher_addr]
        else:
            if (publisher_addr not in self.publishers[topic]):
                self.publishers[topic].append(publisher_addr)
        print("Publisher " + publisher_addr + " registered under topic " + topic)
        print("Current publisher list for topic " + topic + ":")
        print(self.publishers[topic])

    def recv_register1(self):
        register_message_raw = self.register_socket.recv_string()
        #message should come in format: "<pub|sub> <topic> <address>"
        print("Request for registry received!")# + register_message_raw)
        #register_message_parsed = register_message_raw.split(" ")
        #print(register_message_parsed)
        time.sleep(1)
        response_message = self.respond_registry1(register_message_raw)
        print("Response sent: " + response_message)
        self.register_socket.send_string(response_message)
        return

    #could have just checked for option in this function and called respond registry based on which one; oh well
    def recv_register2(self):
        register_message_raw = self.register_socket.recv_string()
        print("Request for registry received!" + register_message_raw)
        time.sleep(1)
        response_message = self.respond_registry2(register_message_raw)
        print("Response sent: " + response_message)
        self.register_socket.send_string(response_message)

    #Literally just forwards the data
    def recv_subscribing1(self):
        message_from_publisher = self.subscribing_socket.recv_string()
        print("Message received: " + message_from_publisher)
        print("Publishing data...")
        self.publishing_socket.send_string(message_from_publisher)
        print("Published!!")
        #Validating data received from publisher
        #(topic, data) = message_from_publisher.split(":")
        #if topic not in self.publishers:
        #    print("ERROR: Topic not registered, data cannot be published")
        #    self.subscribing_socket.send_string("Register before publishing data")
        #    return
        #if pub_ip not in self.publishers[topic]:
        #    print("ERROR: Publisher not registered under topic, data cannot be published")
        #    return
        #self.publish_data(topic, data)
        self.subscribing_socket.send_string("Data published")
        return
    
    #def publish_data(self, topic, data):
    #    print("Publishing data")
    #    pub_str = topic + ":" + data
    #    self.publishing_socket.send_string("%s %s" %(topic, data))
    #    return

    #poll for actions. Possibilities are:
    #   1) register a publisher
    #   2) register a subscriber
    #   3) receive a value and topic
    #       -upon receiving a value and topic, broker should either perform option 1 or 2 - send value back to publisher middleware with a destination to send it to, or send value directly to all subscribers in the topic.
    
    def event_loop1(self):
        print("Starting event loop with option 1...")
        while True:
            events = dict(self.poller.poll())

            if (self.register_socket in events):
                self.recv_register1()
            if self.subscribing_socket in events:
                self.recv_subscribing1()

    def event_loop2(self):
        print("Starting event loop with option 2...")
        while True:
            self.recv_register2()


    #takes in raw regsitry string and returns response message
    def respond_registry1(self, register_message_raw):
        register_message_parsed = register_message_raw.split(" ")
        if (len(register_message_parsed) == 0):
            return "ERROR: no arguments in request"
        print(register_message_parsed)
        if (register_message_parsed[0] == "SUB"):
            return "ACCEPT: Registered Sub with option 1"
        elif (register_message_parsed[0] == "PUB"):
            self.register_pub(register_message_parsed[1], register_message_parsed[2])
            return "ACCEPT: Registered Pub with option 1"
        else:
            return "ERROR: bad arg in request"

    #respond to registration using dissemination method 2
    def respond_registry2(self, register_message_raw):
        register_message_parsed = register_message_raw.split(" ")
        if (len(register_message_parsed) == 0):
            return "ERROR: no arguments in request"
        if (register_message_parsed[0] == "SUB"):
            print(register_message_parsed)
            if (len(register_message_parsed) != 2):
                return "ERROR: incorrect number of arguments in subscriber request"
            #self.register_sub(register_message_parsed[1], register_message_parsed[2]) #register_message_parsed[1] is topic; need to return list of publishers in that topic + dissemination option
            response_str = "ACCEPT: Registered Sub with option 2\n"
            if (register_message_parsed[1] in self.publishers):
                #topic has already been registered; publishers exist so send them. Else don't need to add anything
                for pub in self.publishers[register_message_parsed[1]]:
                    response_str += (pub + ",")
                response_str = response_str[:-1] #slice off last comma
            return response_str
        elif (register_message_parsed[0] == "PUB"):
            if (len(register_message_parsed) != 3):
                    return "ERROR: incorrect number of arguments in publisher request"
            self.register_pub(register_message_parsed[1], register_message_parsed[2])
            self.notify_subscribers(register_message_parsed[1])
            return "ACCEPT: Registered Pub with option 2"
        else:
            return "ERROR: Bad arg in request"

    def notify_subscribers(self, topic):
        publish_message = topic + "\n" #subscribers should filter on their topic
        for pub in self.publishers[topic]:
            publish_message += (pub + ",")
        publish_message = publish_message[:-1]
        print("Notifying subscribers to topic " + topic + " with the message:\n" + publish_message)
        self.notification_socket.send_string(publish_message)
        return


def validate_input():
    usage = "USAGE: python3 broker.py <dissemination option>"
    if (len(sys.argv) != 2):
        print(usage)
        sys.exit(1)
    if (sys.argv[1] != '1' and sys.argv[1] != '2'):
        print("ERROR: Dissemination option must be 1 or 2")



def main():
    validate_input()
    test_broker = Broker(int(sys.argv[1]))
    test_broker.start()

main()
