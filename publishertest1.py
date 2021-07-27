from publisher import Publisher
import time

zkip = '10.0.0.1'
test_publisher = Publisher(zkip)

test_publisher.register('test1', 5)
test_publisher.register('test2', 8)

number = 100

while True:
    input_str = 'test1' + str(number)
    test_publisher.publish('test1', input_str)
    input_str = 'test2' + str(number)
    test_publisher.publish('test2', input_str)

    number = number + 111
    time.sleep(10)
