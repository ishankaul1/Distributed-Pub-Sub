import time
import sys
import zmq
from kazoo.client import KazooClient

class Subscriber:
    def __init__(self, zookeeper_ip):
        self.context = zmq.Context()

        #print("Connecting to broker...")
        #self.register_socket = context.socket(zmq.REQ)

        #self.ip = "localhost" #hardcoded for now; ideally should be the ip of the host this subscriber is running on (check ifconfig and pass as param)

        self.registration_socket = None #map each broker address to the socket connected to it
        self.broker_ip = "" #hardcoded for now; should be ip of the host that is running the broker application
        #self.topic = topic
        self.zookeeper_ip = zookeeper_ip #change to pass this in from cmd line
        self.topic_path = "/topic"
        self.broker_znode = "/broker"
        self.publishers = {}
        #self.subscribing_socket = None #used in opt1
        self.history_len = {} #map topics to history length

        self.subscribing_socket = None # in opt 1 - one socket w multiple filters
        
        self.subscribing_sockets = {}  #in opt2 0 map: key(topic) => val([1 socket for each pub])
        
        self.connection_cut = False
        #connect_str = "tcp://" + self.broker_ip + ":5555"
        #self.register_socket.connect(connect_str)
        #self.subscribing_sockets = None #used in opt2
        self.notification_socket = None #used in opt2
        self.poller = zmq.Poller()
        self.option = None

        self.topics = [] #only used in option 1 - replaced by publishers dict in option 2
        self.publishers = {} # only used in option 2 - topic to publisher list mappings
        self.start_zkclient(zookeeper_ip)


    def start_zkclient(self, zkserver):
        port = '2181'
        url = f'{zkserver}:{port}'
        print(f"connecting to ZK server on '{url}'")
        self.zk = KazooClient(hosts=url)
        self.zk.start()
        if (self.zk.exists(self.broker_znode)):
            #print(self.zk.get(self.broker_znode))
            self.broker_ip = self.zk.get(self.broker_znode)[0].decode('utf-8')
            print("Received broker ip from zookeeper: " + self.broker_ip)
            #connect to the broker
            self.registration_socket = self.context.socket(zmq.REQ)
            connect_str = "tcp://" + self.broker_ip + ":5555"
            self.registration_socket.connect(connect_str)
        

    #starts the subscribe listen
    def start(self):
        print("Starting subscription...")
        #look for broker ip
        if (self.option == 1):
            self.subscribe_listen1()
        elif (self.option == 2):
            self.subscribe_listen2()
        else:
            print("Can't start; please register a topic first")
    
    def get_publisher_data(self):
        print("Getting publisher data from ZK")
        topics = self.zk.get_children(self.topic_path)
        for t in topics:
            if t == self.topic:
                pubs = self.zk.get_children(self.topic_path + '/' + t)
                for p in pubs:
                    data = self.zk.get(self.topic_path + '/' + t + '/' + p)[0].decode('utf-8')
                    self.publishers[p] = {'History': data.split(',')[0], 'Strength': data.split(',')[1]}
        print("List of publishers for topic " + self.topic + ': ')
        print(self.publishers)
    
    def watch_broker_znode_change(self):
        @self.zk.DataWatch(self.broker_znode)
        def reconnect_broker(data, stat):
            self.connection_cut = True
            time.sleep(5)
            self.broker_ip = data.decode('utf-8')
            print(f"Broker IP changed to " + self.broker_ip)
            connect_str = "tcp://" + self.broker_ip + ":5555"
            self.registration_socket.connect(connect_str)
            self.subscribe_listen1(self.broker_ip, self.topic)


    #register with a broker. should be designed extensibly st this subscriber shcan register with several brokers?
    #to do after testing: create new subscription socket that now listens for info
    def register(self, topic, history_len):
        if (history_len < 1):
            print("Cannot receive a history of length < 1")
            return
        self.topic = topic
        self.get_publisher_data()

        #send connection message to broker. Format: "SUB <topic> <my_ip>"
        req_str = "SUB " + topic
        print("Sending request for registry: " + req_str)
        self.registration_socket.send_string(req_str)

        rep_message = self.registration_socket.recv_string()

        if ("ACCEPT: Registered Sub" in rep_message):
            self.history_len[topic] = history_len
            #registry good
            print("Registry Accepted!") 
            #self.topic = topic
            if("2" in rep_message):
                #option 2
                if (self.option is None):
                    self.option = 2
                print("Connecting to broker option 2 with topic '" + topic + "'")
                publisher_list_raw = self.publishers.keys()
                self.pub_select = self.parse_publisher_list(publisher_list_raw)
                if self.pub_select != Null:
                    self.subscribe_listen2(self.broker_ip, self.topic, self.pub_select)
                else:
                    print("No publisher selected from list")
                    sys.exit(1)

                #put each publisher in subscribing socket and publisher mappingif doesn't already exist
                if topic not in self.subscribing_sockets:
                    #create new socket, then connect new socket to each publisher not already in publisher list
                    self.subscribing_sockets[topic] = []
                    new_socket = self.context.socket(zmq.SUB)
                    connect_str = "tcp://" + self.pub_select + ":5556"
                    new_socket.connect(connect_str)
                    new_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
                    self.poller.register(new_socket, zmq.POLLIN)
            else:
                #option 1
                if (self.option is None):
                    self.option = 1
                print("Connecting to broker option 1 with topic '" + topic + "'")
                if not self.subscribing_socket:
                    self.subscribing_socket = self.context.socket(zmq.SUB)
                    connect_str = "tcp://" + self.broker_ip + ":5557"
                    self.subscribing_socket.connect(connect_str)
                if topic not in self.topics:
                    self.subscribing_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
                    self.topics.append(topic)
                print("Connected!")



                #self.watch_broker_znode_change()
                #self.subscribe_listen1(self.broker_ip, self.topic)
            #start listening to for published events
        else:
            print("Error. Response: " + rep_message)

    def subscribe_listen1(self):
        print("Now Listening to " + self.broker_ip + " for all registered topics")
        print("Topics: " + ','.join(self.topics) + "\n")
        #connect_str = "tcp://" + self.broker_ip + ":5557"
        #self.subscribing_socket.connect(connect_str)
        #self.subscribing_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
        while self.zk.get(self.broker_znode)[0].decode('utf-8') == self.broker_ip:
            time.sleep(10)
            try:
                subs_data = self.subscribing_socket.recv_string( flags = zmq.NOBLOCK )
                topic, raw_data = subs_data.split(':')
                if (',' in raw_data):
                    data = raw_data.split(',')
                else:
                    data = [raw_data]
                if (len(raw_data) < self.history_len[topic]):
                    #publisher sending less history than we're asking for; just take it all
                    print("Data received from topic '" + topic + "':")
                    print(','.join(data))
                else:
                    #must only keep the last 'history_len' values
                    data = data[-self.history_len[topic]:]
                    print("Data received from topic '" + topic + "':")
                    print(','.join(data))

            except zmq.ZMQError as e:
                if e.errno == zmq.EAGAIN:
                    pass
        self.broker_ip = self.zk.get(self.broker_znode)[0].decode('utf-8')
        self.subscribe_listen1()
    
    def subscribe_listen2(self):
        if (self.notification_socket is None):
            self.notification_socket = self.context.socket(zmq.SUB) #socket for receiving new info about publishers
        #if (self.subscribing_sockets is None):
        #    self.subscribing_sockets = {} #empty dict to hold all <pub_ip, socket> pairs
        notif_connect_str = "tcp://" + self.broker_ip + ":5557"
        self.notification_socket.connect(notif_connect_str)
        for topic in self.publishers:
            self.notification_socket.setsockopt_string(zmq.SUBSCRIBE, topic) #subscribe to publisher notifications on this topic

        #create poller and register notification socket with it
        #self.poller = zmq.Poller()
        self.poller.register(self.notification_socket, zmq.POLLIN)
        while self.zk.exists(self.topic_path + '/' + self.topic + '/' + self.pub_select):
            events = dict(self.poller.poll())
            print(events)
            #receive notification event
            if (self.notification_socket in events):
                publishers = self.recv_notification()
            #receive on each subscribing socket 
            for pub_ip in self.subscribing_sockets:
                if self.subscribing_sockets[pub_ip] in events:
                    self.recv_sub_socket(self.subscribing_sockets[pub_ip])
        publisher_list_raw = self.publishers.keys()
        self.pub_select = self.parse_publisher_list(publisher_list_raw)
        if self.pub_select != NULL:
            self.subscribe_listen2(self.broker_ip, self.topic, self.pub_select)
        else:
            print("No publisher selected from list")
            sys.exit(1) 

    #TODO: option 2 - receive notification of new publisher on notification socket
    def recv_notification(self):
        notif_raw = self.notification_socket.recv_string(zmq.DONTWAIT)
        print("Notification received: " + notif_raw + "\n")
        topic, pub_list_raw = notif_raw.split('\n')
        if (',' in pub_list_raw):
            pub_list = pub_list_raw.split(',')
        else:
            pub_list = [pub_list_raw]
        
        print("Creating new publishing sockets...\n")
        new_publishers = [pub for pub in pub_list if pub not in self.publishers[topic]]
        socket = self.subscribing_sockets[topic]
        for pub_ip in new_publishers:
            connect_str = "tcp://" + pub_ip + ":5556"
            socket.connect(connect_str)
            self.publishers[topic].append(pub_ip)
        print("Added new publishers: " + ','.join(new_publishers))

    def add_pub_opt2(self, pub_ip):
        if (pub_ip in self.subscribing_sockets):
            print("Socket for publisher at " + pub_ip + " already exists")
        else:
            new_socket = self.context.socket(zmq.SUB)
            connect_str = "tcp://" + pub_ip + ":5556"
            new_socket.connect(connect_str)
            new_socket.setsockopt_string(zmq.SUBSCRIBE, self.topic)
            self.poller.register(new_socket, zmq.POLLIN)
            self.subscribing_sockets[pub_ip] = new_socket
            print("Connected a new subscribing socket to publisher " + pub_ip)
    
    #TODO: option 2 - receive data from a publisher
    def recv_sub_socket(self, socket, topic):
        subs_data = socket.recv_string(zmq.DONTWAIT)
        print(subs_data)
        topic,raw_data = subs_data.split(':')
        if (',' in raw_data):
            data = raw_data.split(',')
        else:
            data = [raw_data]
        if (len(raw_data) < self.history_len[topic]):
            #publisher sending less history than we're asking for; just take it all
            print("Data received from topic '" + topic + "':")
            print(','.join(data))
        else:
            #must only keep the last 'history_len' values
            data = data[-self.history_len[topic]:]
            print("Data received from topic '" + topic + "':")
            print(','.join(data))
    
    def parse_publisher_list(self, publisher_list_raw):
        for p in publisher_list_raw:
            if self.publishers[p]['History'] >= self.history:
                return p
        print("No publishers with matched with history threshold")
        return NULL

    #def run():
    #    if self.validate_input():
    #        self.register(self.broker_ip, self.topic)


def validate_input():
        usage = "python3 subscriber.py <zookeeper ip>"
        if len(sys.argv) < 2:
            print(usage)
            sys.exit(1)
        return True
            
def main():
    #test that this stuff works
    if (validate_input()):
        test_subscriber = Subscriber(sys.argv[1])
        test_subscriber.register('t1', 5)
        test_subscriber.register('t2', 3)
        test_subscriber.start()
main()
