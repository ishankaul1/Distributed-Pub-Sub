from subscriber import Subscriber

zkip = '10.0.0.1'

test_subscriber = Subscriber(zkip)
test_subscriber.register('test1', 7)
test_subscriber.register('test2', 10)

test_subscriber.start()