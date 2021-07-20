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

        self.start_zk()
        #self.start()


    def start_zk(self):
        port = '2181'
        url = f'{self.zookeeper_ip}:{port}'
        print(f"connecting to ZK server on '{url}'")
        self.zk = KazooClient(hosts=url)
        self.zk.start()
        print("Zkclient started!\n")
        print(f"Publisher: {self.ip}\n")
        #look for broker ip
        if (self.zk.exists(self.broker_znode)):
            #print(self.zk.get(self.broker_znode))
            self.broker_ip = self.zk.get(self.broker_znode)[0].decode('utf-8')
            print("Received broker ip from zookeeper: " + self.broker_ip)
            self.connect_register_socket()
            # self.registration_socket = self.context.socket(zmq.REQ)
            # connect_str = "tcp://" + self.broker_ip + ":5555"
            # self.registration_socket.connect(connect_str)
            print("Connected to broker\n")
            if (self.option == 1):
                self.create_publishing_socket1()
            else:
                self.create_publishing_socket2()
            #self.register(4)
            self.watch_broker_znode_change()
            
    def connect_register_socket(self):
        self.registration_socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + self.broker_ip + ":5555"
        self.registration_socket.connect(connect_str)

    def watch_broker_znode_change(self):
        @self.zk.DataWatch(self.broker_znode)
        def reconnect_broker(data, stat):
            self.broker_ip = data.decode('utf-8')
            print(f"Broker IP changed to " + self.broker_ip)
            connect_str = "tcp://" + self.broker_ip + ":5555"
            self.registration_socket.connect(connect_str)


    def register(self, topic, history_len):
        if topic in self.topics:
            print("Already registered to topic")
            return
        if (history_len < 1):
            print("Cannot hold a history of length < 1")
            return
    
        # self.registration_socket = self.context.socket(zmq.REQ)
        # connect_str = "tcp://" + self.broker_ip + ":5555"
        # self.registration_socket.connect(connect_str)

        #send registration request to broker. Format: "PUB <topic> <my_ip>"
        req_str = "PUB " + topic + " " + self.ip
        print("Sending request for registration: " + req_str)
        self.registration_socket.send_string(req_str)

        rep_message = self.registration_socket.recv_string()

        if ("ACCEPT: Registered Pub" in rep_message):
            self.topics.append(topic)
            self.topic_historylen_dict[topic] = history_len
            #registry good
            print("Registry Accepted! Can now publish to topic '" + topic + "'")
            #TO DO: parse out last char, set option, and use to decide type of socket
            if self.option is None:
                self.option = int(rep_message[-1])
            
            #if (self.publishing_socket is None):
            #    if (self.option == 1):
            #        self.create_publishing_socket1()
            #    else:
            #        self.create_publishing_socket2()
            #self.create_publishing_socket()
        else:
            print("Error. Response: " + rep_message)
        self.connect_register_socket() #have to reconnect
    
    def create_publishing_socket1(self):
        self.publishing_socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + self.broker_ip + ":5556"
        self.publishing_socket.connect(connect_str)

    def create_publishing_socket2(self):
        self.publishing_socket = self.context.socket(zmq.PUB)
        connect_str = "tcp://*:5556" #might have to change to own ip? not sure
        self.publishing_socket.bind(connect_str)

    def publish(self, topic, data):
        #new_socket = self.context.socket(zmq.REQ)
        #connect_str = "tcp://" + self.broker_ip + ":5556"
        #new_socket.connect(connect_str)

        if (self.option != 1 and self.option != 2):
            print("ERROR: Option not set correctly. Can't publish\n")
            return
        if (topic not in self.topics):
            print("ERROR: Topic not been registered. Can't publish\n")
            return
        if (self.publishing_socket is None):
            print("ERROR: Publishing socket not yet created")

   
        #append to history
        if (topic not in self.topic_rollinghistory_dict):
            self.topic_rollinghistory_dict[topic] = [data]
            window_data = self.topic_rollinghistory_dict[topic][0]
        else: 
            self.topic_rollinghistory_dict[topic].append(data)
            if len(self.topic_rollinghistory_dict[topic]) > self.topic_historylen_dict[topic]:
            #history at max capacity; slice off first value to maintain desired size of window
                self.topic_rollinghistory_dict[topic] = self.topic_rollinghistory_dict[topic][1:]
            window_data = ','.join(self.topic_rollinghistory_dict[topic])
        
        # if (len(self.rolling_history) > 1):
        #     window_data = ','.join(self.rolling_history)
        # else:
        #     window_data = self.rolling_history[0]


        #Sending data to be published
        pub_str = topic + ":" + window_data
        print("Publishing topic: " + topic + " and data: " + window_data)
        self.publishing_socket.send_string(pub_str)
        #recreate publishing socket
        if (self.option == 1):
            self.create_publishing_socket1()

    #def getTopic(self):
    #    return self.topic

        #publish_response = new_socket.recv_string()
        #print(publish_response)
    
    #def run(self):
    #    if self.validate_input():
    #        if sys.argv[1] == "register":
    #            self.register(self.broker_ip, self.topic)
    #        elif sys.argv[1] == "publish":
    #            self.publish(self.broker_ip, self.topic, self.data)


def validate_input():
    usage = "python3 publisher.py <broker ip>"
    if len(sys.argv) < 2:
        print(usage)
        sys.exit(1)
    #elif (sys.argv[1] == "publish" and len(sys.argv) < 5):
    #    print("data is needed for publishing")
    #    sys.exit(1)
        
    #if sys.argv[1] == "register":
    #    self.action = sys.argv[1]
    #    self.broker_ip = sys.argv[2]
    #    self.topic = sys.argv[3]
    #elif sys.argv[1] == "publish":
    #    self.action = sys.argv[1]
    #    self.broker_ip = sys.argv[2]
    #    self.topic = sys.argv[3]
    #    self.data =sys.argv[4]
    #return True

#Treat as a unit test
def main():
    validate_input()
    
    test_publisher = Publisher(sys.argv[1])

    test_publisher.register('test1', 4)
    test_publisher.register('test2', 6)
    #test_publisher.register()
    #input_str = ""
    number = 100
    while True:#(input_str != "exit"):
        #input_str = input("Enter data to be sent on topic '" + test_publisher.getTopic() + "', or type 'exit' to disconnect:\n")
        input_str = 'test1' + str(number)
        test_publisher.publish('test1', input_str)
        input_str = 'test2' + str(number)
        test_publisher.publish('test2', input_str)

        number = number + 111
        time.sleep(10)
        #if (input_str != "exit"):
        

    #print("Disconnected")
    # test_publisher.register("localhost", "porcupines")
    # test_publisher.register("localhost", "dunder")
    # # test_publisher.register("localhost", "tesla")
    # # test_publisher.register("localhost", "jolt")
    # test_publisher.publish("localhost", "porcupines", "test1")
    # test_publisher.publish("localhost", "dunder", "test1")
    # test_publisher.publish("localhost", "porc", "test1")

main()
