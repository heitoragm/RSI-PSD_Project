import sys, pika

# if len(sys.argv) != 5:
# 	print >> sys.stderr, "Usage: %s [RabbitMQ host] [RabbitMQ user] [RabbitMQ password] [RabbitMQ vhost]..." % (sys.argv[0])
# 	sys.exit(1)
host = "192.168.0.103" #sys.argv[1]
user = "EHL" #sys.argv[2]
passwd = "123" #sys.argv[3]
vhost = "RSIPSD" #sys.argv[4]

credentials = pika.PlainCredentials(user, passwd)
connection = pika.BlockingConnection(pika.ConnectionParameters(host, 5672, vhost, credentials))
channel = connection.channel()
channel.exchange_declare(exchange='rsipsd_project',
                         exchange_type='topic')

result = channel.queue_declare('',exclusive=False)
queue_name = result.method.queue

channel.queue_bind(exchange='rsipsd_project',
                       queue=queue_name,
                       routing_key='probe')

print ' [*] Waiting for logs. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print " [x] %r:%r" % (method.routing_key, body,)

channel.basic_consume(on_message_callback=callback, queue=queue_name, auto_ack=True)

channel.start_consuming()