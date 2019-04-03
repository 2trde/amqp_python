import pika
import json

import traceback
import os
import time

def log(str):
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()) + ':  ' + str, flush = True)

class AmqpEndpoint:
    def __init__(self, exchange, request_topic, response_topic, on_receive,
        queue_name = None, amqp_connection = None, reconnect = True, on_error = None):
        '''
            Provides an amqp endpoint that listens to request incoming from
            exchange under request_topic and sends responses to
            the same exchange under response_topic.
            Incoming messages are handled by calling the function on_receive.
            You can start the service by calling .run().
            It is recommended to provide a queue_name (default is f'{exchange}.{topic}')
            as well as an amqp_connection (default is to attempt to read it from the environment).
            If you do not want the service to reconnect after the connection has
            been closed, set reconnect to False.
        '''
        if amqp_connection is None:
            amqp_connection = os.environ.get('AMQP_CONNECTION')
        if queue_name is None:
            queue_name = '.'.join([exchange, request_topic])
        
        self.queue_name = queue_name
        self.exchange = exchange
        self.request_topic = request_topic
        self.response_topic = response_topic
        self.reconnect = reconnect
        self.on_receive = on_receive
        self.on_error = on_error
        self.parameters = pika.URLParameters(amqp_connection)
    
    def run(self):
        while True:
            try:
                self._setup()
                log('Service has been started, listening on queue {}.'.format(self.queue_name))
                self.channel.start_consuming()
                # this is blocking until all consumers are canceled
                log('The consumer has been canceled, closing the connection ...')
                self.connection.close()
            except pika.exceptions.ConnectionClosed:
                log('The connection has been closed.')
            
            if self.reconnect:
                time.sleep(1)
                log('Reconnecting ...')
            else:
                log('Shutting down.')
                break
    
    def _setup(self):
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue = self.queue_name, durable=True)
        self.channel.exchange_declare(exchange = self.exchange, exchange_type='topic')
        self.channel.queue_bind(self.queue_name, self.exchange, self.request_topic)
        self.consumer_tag = self.channel.basic_consume(queue = self.queue_name, on_message_callback = self._consume)
    
    def _consume(self, channel, method, properties, body):
        log('Received a new message.')
        self.channel.basic_ack(method.delivery_tag)
        response = self._process_request(body)
        if response is not None:
            # delivery_mode = 2 -> persistent
            self.channel.basic_publish(self.exchange, self.response_topic, response, pika.BasicProperties(content_type='text/json', delivery_mode=2))
            log('Sent response to exchange {}, topic {}.'.format(self.exchange, self.response_topic))
    
    def _process_request(self, body):
        try:
            start_time = time.time()
            request = json.loads(body.decode('utf-8'))
            response = json.dumps(self.on_receive(request))
            elapsed_time = time.time() - start_time
            log('Successfully processed request, took {} seconds.'.format(elapsed_time))
            return response
        except Exception as error:
            log('Unexpected error: {}'.format(error))
            print(traceback.format_exc())
        try:
            log('Attempting to send on_error response instead ...')
            return json.dumps(self.on_error(request))
        except:
            log('Failed to process request.')
            return None
