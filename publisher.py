import sys
import zmq
import time
import netifaces
from kazoo.client import KazooClient
import netifaces

class Publisher:
    def __init__(self, zookeeper_ip):
        self.context = zmq.Context()

        self.ip = netifaces.ifaddresses(netifaces.interfaces()[1])[netifaces.AF_INET][0]['addr']
        self.broker_ip = None#broker_ip
        #self.data = ""
        self.zookeeper_ip = zookeeper_ip
        self.topics = []
        self.registration_socket = None
        self.publishing_socket = None
        self.option = None #dissemination option - gets decided upon receiving response from broker registration
        #self.connected_sockets = {} #for extending to connecting to multiple brokers
        self.broker_znode = "/broker"
        self.topic_historylen_dict = {}
        self.topic_rollinghistory_dict = {}

        self.topics_path = "/topics" #one znode per topic, underneath which each topic
        #has 1 child per publisher registered to it. Calculate ownership strength in
        #first come first serve order. Hitory length and ownership strength stored in 
        #the publisher's znode

        self.start_zk()

    def start_zk(self):
        port = '2181'
        url = f'{self.zookeeper_ip}:{port}'
        print(f"connecting to ZK server on '{url}'")
        self.zk = KazooClient(hosts=url)
        self.zk.start()
        print("Zkclient started!\n")
        print(f"Publisher: {self.ip}\n")

        #Connect to broker
        if (self.zk.exists(self.broker_znode)):
            self.broker_ip = self.zk.get(self.broker_znode)[0].decode('utf-8')
            print("Received broker ip from zookeeper: " + self.broker_ip)
            self.connect_register_socket()
            print("Connected to broker\n")
            if (self.option == 1):
                self.create_publishing_socket1()
            else:
                self.create_publishing_socket2()
            self.watch_broker_znode_change()
            
    def connect_register_socket(self):
        self.registration_socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + self.broker_ip + ":5555"
        self.registration_socket.connect(connect_str)

    #Fault tolerance on broker death
    def watch_broker_znode_change(self):
        @self.zk.DataWatch(self.broker_znode)
        def reconnect_broker(data, stat):
            self.broker_ip = data.decode('utf-8')
            print(f"Broker IP changed to " + self.broker_ip)
            connect_str = "tcp://" + self.broker_ip + ":5555"
            self.registration_socket.connect(connect_str)
            #REPLACING THIS PART WITH PERSISTENT ZNODE DATA STRUCTS:
            #re-register so that new broker knows what topics and pubs are subscribed
            #for topic in self.topics:
            #    self.register(topic, self.topic_historylen_dict[topic])


    def register(self, topic, history_len):
        #if topic in self.topics:
        #    print("Already registered to topic")
        #    return
        if (history_len < 1):
            print("Cannot hold a history of length < 1")
            return
    
        #Send registration request to broker. Format: "PUB <topic> <my_ip>"
        req_str = "PUB " + topic + " " + self.ip
        print("Sending request for registration: " + req_str)
        self.registration_socket.send_string(req_str)

        rep_message = self.registration_socket.recv_string()

        if ("ACCEPT: Registered Pub" in rep_message):
            #Able to register to this topic
            #Persist data in zookeeper and self

            self.persist_accepted_registration(topic, history_len)
            if self.option is None:
                self.option = int(rep_message[-1])

            print("Registry Accepted! Can now publish to topic '" + topic + "'")
            #Get type of socket from response if not already determined
            
        else:
            print("Error. Response: " + rep_message)
        #Need to reconnect pub sockets
        self.connect_register_socket() 
    
    #persist necessary information in zookeeper and self
    #structure should be: /topics -> /topic_name -> /publisher_name
    def persist_accepted_registration(self, topic, history_len):
        topic_path_str = self.topics_path + "/" + topic
        #if no '/topics' has been created yet we need to create 
        if not self.zk.exists(self.topics_path):
            self.zk.create(self.topics_path, ephemeral=False)
        
        topics = self.zk.get_children(self.topics_path)

        print("Current topics: " + ','.join(topics))

        if (len(topics) == 0 or topic not in topics):
            self.zk.create(topic_path_str, ephemeral=False)

        current_pubs = self.zk.get_children(topic_path_str)
        print("Current Pubs: " + ','.join(current_pubs))
        if self.ip not in current_pubs:
            #create new znode; name = self.ip, data = ownership_strength:history_len
            ownership_strength = len(current_pubs)+1
            new_value = str(ownership_strength) + ',' + str(history_len)
            new_path = topic_path_str + "/" + self.ip
            self.zk.create(new_path , ephemeral=True, value=new_value.encode('utf-8'))

        if (topic not in self.topics):
                self.topics.append(topic)
                self.topic_historylen_dict[topic] = history_len

    def create_publishing_socket1(self):
        self.publishing_socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + self.broker_ip + ":5556"
        self.publishing_socket.connect(connect_str)

    def create_publishing_socket2(self):
        self.publishing_socket = self.context.socket(zmq.PUB)
        connect_str = "tcp://*:5556" #might have to change to own ip? not sure
        self.publishing_socket.bind(connect_str)

    def publish(self, topic, data):

        if (self.option != 1 and self.option != 2):
            print("ERROR: Option not set correctly. Can't publish\n")
            return
        if (topic not in self.topics):
            print("ERROR: Topic not been registered. Can't publish\n")
            return
        if (self.publishing_socket is None):
            print("ERROR: Publishing socket not yet created")

   
        #Updates sliding window of history associated to the topic, then publishes
        if (topic not in self.topic_rollinghistory_dict):
            self.topic_rollinghistory_dict[topic] = [data]
            window_data = self.topic_rollinghistory_dict[topic][0]
        else: 
            self.topic_rollinghistory_dict[topic].append(data)
            if len(self.topic_rollinghistory_dict[topic]) > self.topic_historylen_dict[topic]:
            #history at max capacity; slice off first value to maintain desired size of window
                self.topic_rollinghistory_dict[topic] = self.topic_rollinghistory_dict[topic][1:]
            window_data = ','.join(self.topic_rollinghistory_dict[topic])
        
        #Sending data to be published
        pub_str = topic + ":" + self.ip + ":" + window_data
        print("Publishing topic: " + topic + " and data: " + window_data)
        self.publishing_socket.send_string(pub_str)
        #recreate publishing socket
        if (self.option == 1):
            self.create_publishing_socket1()

def validate_input():
    usage = "python3 publisher.py <broker ip>"
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)

#Unit test for this module. Registers to two topics with different length histories, and publishes to both.
#Can import this module and run in a similar fashion to test differently.
def main():
    validate_input()
    
    test_publisher = Publisher(sys.argv[1])

    test_publisher.register('test1', 4)
    test_publisher.register('test2', 6)

    number = 100
    while True:
        input_str = 'test1' + str(number)
        test_publisher.publish('test1', input_str)
        input_str = 'test2' + str(number)
        test_publisher.publish('test2', input_str)

        number = number + 111
        time.sleep(10)

#main()
