import sys
import zmq
import time
import netifaces
from kazoo.client import KazooClient
import netifaces

class Publisher:
    def __init__(self, zookeeper_ip, topic):
        self.context = zmq.Context()

        self.ip = netifaces.ifaddresses(netifaces.interfaces()[1])[netifaces.AF_INET][0]['addr']
        self.broker_ip = None#broker_ip
        #self.data = ""
        self.zookeeper_ip = zookeeper_ip
        self.topic = topic
        self.registration_socket = None
        self.publishing_socket = None
        self.option = None #dissemination option - gets decided upon receiving response from broker registration
        #self.connected_sockets = {} #for extending to connecting to multiple brokers
        self.broker_znode = "/broker"
        self.publish_znode = self.broker_znode + '/' + self.topic

        self.zk = self.start_zkclient(zookeeper_ip)
        self.start()


    def start_zkclient(self, zkserver):
        port = '2181'
        url = f'{zkserver}:{port}'
        print(f"connecting to ZK server on '{url}'")
        zk = KazooClient(hosts=url)
        zk.start()
        return zk

    def start(self):
        print(f"Publisher: {self.ip}")
        #look for broker ip
        if (self.zk.exists(self.broker_znode)):
            #print(self.zk.get(self.broker_znode))
            self.broker_ip = self.zk.get(self.broker_znode)[0].decode('utf-8')
            print("Received broker ip from zookeeper: " + self.broker_ip)
            self.register()
            self.watch_broker_znode_change()
            print("now doing publish elect")
            if not self.zk.exists(self.publish_znode):
                self.zk.create(self.publish_znode)
            #print("Awaiting for Election Results")
            election = self.zk.Election(self.publish_znode, self.ip)
            print("Contendors for election")
            print(election.contenders())
            election.run(self.publish)
        else:
            print("broker znode does not exist")

    def watch_broker_znode_change(self):
        @self.zk.DataWatch(self.broker_znode)
        def reconnect_broker(data, stat):
            self.broker_ip = data.decode('utf-8')
            print(f"Broker IP changed to " + self.broker_ip)
            connect_str = "tcp://" + self.broker_ip + ":5555"
            self.registration_socket.connect(connect_str)

    def register(self):
        self.registration_socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + self.broker_ip + ":5555"
        self.registration_socket.connect(connect_str)

        #send registration request to broker. Format: "PUB <topic> <my_ip>"
        req_str = "PUB " + self.topic + " " + self.ip
        print("sending request for registration: " + req_str)
        self.registration_socket.send_string(req_str)

        rep_message = self.registration_socket.recv_string()

        if ("ACCEPT: Registered Pub" in rep_message):
            #registry good
            print("Registry Accepted! Creating new publishing socket...")
            #TO DO: parse out last char, set option, and use to decide type of socket
            self.option = int(rep_message[-1])
            
            if (self.option == 1):
                self.create_publishing_socket1()
            else:
                self.create_publishing_socket2()
            #self.create_publishing_socket()
        else:
            print("Error. Response: " + rep_message)
    
    def create_publishing_socket1(self):
        self.publishing_socket = self.context.socket(zmq.REQ)
        connect_str = "tcp://" + self.broker_ip + ":5556"
        self.publishing_socket.connect(connect_str)

    def create_publishing_socket2(self):
        self.publishing_socket = self.context.socket(zmq.PUB)
        connect_str = "tcp://*:5556" #might have to change to own ip? not sure
        self.publishing_socket.bind(connect_str)

    def publish(self):
        print("Publisher for %s is now %s" % (self.topic,self.ip))
        if (self.option != 1 and self.option != 2):
            print("ERROR: Option not set correctly. Can't publish")
            return
        num = 111
        data = self.topic + str(num)
        while(True):
            time.sleep(10)
        #Sending data to be published
            pub_str = self.topic + ":" + self.ip + ":" + data
            print("Sending topic: " + self.topic + " and data: " + data + " to " + self.broker_ip )
            self.publishing_socket.send_string(pub_str)
            num += 111
            data = self.topic + str(num)
            #recreate publishing socket
            if (self.option == 1):
                self.create_publishing_socket1()

    def getTopic(self):
        return self.topic

def validate_input():
    usage = "python3 publisher.py <broker ip> <topic>"
    if len(sys.argv) < 3:
        print(usage)
        sys.exit(1)


def main():
    validate_input()
    test_publisher = Publisher(sys.argv[1], sys.argv[2])
    #test_publisher.register()

    print("Disconnected")

main()