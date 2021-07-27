from subscriber import Subscriber

zkip = '10.0.0.1'

test_subscriber = Subscriber(zkip)
test_subscriber.register('test1', 4)
test_subscriber.register('test2', 3)

test_subscriber.start()