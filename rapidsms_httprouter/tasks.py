import StringIO
from celery.task import task
from datetime import datetime, timedelta
from django.conf import settings
from .models import Message, DeliveryError
from .router import HttpRouter
from urllib import quote_plus, unquote
from urllib2 import urlopen
import urllib2
import traceback
import time
import re
import redis

import logging
logger = logging.getLogger(__name__)

def fetch_url(url, params):
    if hasattr(settings, 'ROUTER_FETCH_URL'):
        fetch_url = HttpRouter.definition_from_string(getattr(settings, 'ROUTER_FETCH_URL'))
        return fetch_url(url, params)
    else:
        return HttpRouter.fetch_url(url, params)

def build_send_url(params, **kwargs):
    """
    Constructs an appropriate send url for the given message.
    """
    # make sure our parameters are URL encoded
    params.update(kwargs)
    for k, v in params.items():
        try:
            params[k] = quote_plus(str(v))
        except UnicodeEncodeError:
            params[k] = quote_plus(str(v.encode('UTF-8')))
            
    # get our router URL
    router_url = settings.ROUTER_URL

    # is this actually a dict?  if so, we want to look up the appropriate backend
    if type(router_url) is dict:
        router_dict = router_url
        backend_name = params['backend']
            
        # is there an entry for this backend?
        if backend_name in router_dict:
            router_url = router_dict[backend_name]

        # if not, look for a default backend 
        elif 'default' in router_dict:
            router_url = router_dict['default']

        # none?  blow the hell up
        else:
            raise Exception("No router url mapping found for backend '%s', check your settings.ROUTER_URL setting" % backend_name)

    # return our built up url with all our variables substituted in
    full_url = router_url % params
    return full_url

def send_message(msg, **kwargs):
    """
    Sends a message using its configured endpoint
    """
    msg_log = "Sending message: [%d]\n" % msg.id

    print "[%d] >> %s\n" % (msg.id, msg.text)

    # and actually hand the message off to our router URL
    try:
        params = {
            'backend': msg.connection.backend.name,
            'recipient': msg.connection.identity,
            'text': msg.text,
            'id': msg.pk
        }

        url = build_send_url(params)
        print "[%d] - %s\n" % (msg.id, url)
        msg_log += "%s %s\n" % (msg.connection.backend.name, url)

        response = fetch_url(url, params)
        status_code = response.getcode()

        body = response.read().decode('ascii', 'ignore').encode('ascii')

        msg_log += "Status Code: %d\n" % status_code
        msg_log += "Body: %s\n" % body

        # kannel likes to send 202 responses, really any
        # 2xx value means things went okay
        if int(status_code/100) == 2:
            print "  [%d] - sent %d" % (msg.id, status_code)
            logger.info("SMS[%d] SENT" % msg.id)
            msg.sent = datetime.now()
            msg.status = 'S'
            msg.save()

            return body
        else:
            raise Exception("Received status code: %d" % status_code)
    except Exception as e:
        print "  [%d] - send error - %s" % (msg.id, str(e))

        # previous errors
        previous_count = DeliveryError.objects.filter(message=msg).count()
        msg_log += "Failure #%d\n\n" % (previous_count+1)
        msg_log += "Error: %s\n\n" % str(e)
        
        if previous_count >= 2:
            msg_log += "Permanent failure, will not retry."
            msg.status = 'F'
            msg.save()
        else:
            msg_log += "Will retry %d more time(s)." % (2 - previous_count)
            msg.status = 'E'
            msg.save()

        DeliveryError.objects.create(message=msg, log=msg_log)

    return None

@task(track_started=True)
def send_message_task(message_id):  #pragma: no cover
    # noop if there is no ROUTER_URL
    if not getattr(settings, 'ROUTER_URL', None):
        print "  [%d] - no ROUTER_URL configured, ignoring" % message_id

    # we use redis to acquire a global lock based on our settings key
    r = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

    # try to acquire a lock, at most it will last 60 seconds
    with r.lock('send_message_%d' % message_id, timeout=60):
        print "  [%d] - sending message" % message_id

        # get the message
        msg = Message.objects.get(pk=message_id)

        # if it hasn't been sent and it needs to be sent
        if msg.status == 'Q' or msg.status == 'E':
            body = send_message(msg)
            print "  [%d] - msg sent status: %s" % (message_id, msg.status)

@task(track_started=True)
def resend_errored_messages_task():  #pragma: no cover
    # noop if there is no ROUTER_URL
    if not getattr(settings, 'ROUTER_URL', None):
        print "--resending errors-- no ROUTER_URL configured, ignoring"

    print "-- resending errors --"

    # we use redis to acquire a global lock based on our settings key
    r = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)

    # try to acquire a lock, at most it will last 5 mins
    with r.lock('resend_messages', timeout=300):
        # get all errored outgoing messages
        pending = Message.objects.filter(direction='O', status__in=('E'))

        # send each
        count = 0
        for msg in pending:
            msg.send()
            count+=1

            if count >= 100: break

        print "-- resent %d errored messages --" % count

        # and all queued messages that are older than 2 minutes
#        two_minutes_ago = datetime.now() - timedelta(minutes=2)
#        pending = Message.objects.filter(direction='O', status__in=('Q'), updated__lte=two_minutes_ago)
        pending = Message.objects.filter(direction='O', status__in=('Q')) #why wait for two minutes???

        # send each
        count = 0
        for msg in pending:
            msg.send()
            count+=1

            if count >= 100: break

        print "-- resent %d pending messages -- " % count

@task(track_started=True)
def queue_messages_task():
    """
    Queue batched messages
    TODO: Ensure that batches that were queued before are taken care of first
    """
    from .models import Message, MessageBatch
    from rapidsms.messages.outgoing import OutgoingMessage
    batches = MessageBatch.objects.filter(status='Q')
    for batch in batches:
        try:
            messages = Message.objects.filter(batch=batch, status='P', direction='O')[0:settings.CHUNK_SIZE]
            # create a RapidSMS outgoing message
            for outgoing in messages:
                msg = OutgoingMessage(outgoing.connection, outgoing.text.replace('%','%%'))
                msg.db_message = outgoing
                if msg:
                    outgoing.status = 'Q'
                    outgoing.save()
        except IndexError:
            pass

def build_send_url_legacy(params, **kwargs):
    """
    Constructs an appropriate send url for the given message.
    """

    # make sure our parameters are URL encoded
    params.update(kwargs)
    for k, v in params.items():
        try:
            params[k] = quote_plus(str(v))
        except UnicodeEncodeError:
            params[k] = quote_plus(str(v.encode('UTF-8')))

    # is this actually a dict?  if so, we want to look up the appropriate backend
    router_url = params.get('router_url', None)
    backend = params.get('backend', None)
    if type(router_url) is dict:
        router_dict = router_url
        backend_name = backend

        # is there an entry for this backend?
        if backend_name in router_dict:
            router_url = router_dict[backend_name]

        # if not, look for a default backend 
        elif 'default' in router_dict:
            router_url = router_dict['default']

        # none?  blow the hell up
        else:
            raise Exception("No router url mapping found for backend '%s', check your settings.ROUTER_URL setting" % backend_name)

    # return our built up url with all our variables substituted in
    # decode the url again
    router_url = unquote(router_url).decode('utf8')
    full_url = router_url % params

    return full_url
    
def send_backend_chunk(router_url, pks, backend_name):
    msgs = Message.objects.filter(pk__in=pks).exclude(connection__identity__iregex="[a-z]")
    print "-- %s messages to be send out -- " % msgs.count()
    #        try:
    params = {
    'router_url': router_url,
     'backend': backend_name, 
     'recipient': ' '.join(msgs.values_list('connection__identity', flat=True)), 
     'text': msgs[0].text, 
    }
    url = build_send_url_legacy(params)
    print "-- calling url: %s -- " % url
    res = None 
    try:
        res = urllib2.urlopen(url, timeout=15)
    except urllib2.HTTPError, err:
        if err.code == 404:
            print " -- Not found! -- Kannel might be down"
        elif err.code == 403:
            print "-- Access denied! -- Connection to Kannel Refused"
        else:
            print "Something wrong happened! Error code", err.code
    except urllib2.URLError, err:
        print "Some other error happened:", err.reason
    if res:
        status_code = res.get_code()
    else:
        try:
            status_code = err.code
        except AttributeError:
            status_code = err.reason
    
    print "-- kannel responded with %s status code -- " % status_code
    
    # kannel likes to send 202 responses, really any
    # 2xx value means things went okay
    if not res == None and int(status_code / 100) == 2:
        msgs.update(status='S')
        print "-- kannel accepted all the %s messages, we mark them as sent -- " % msgs.count()
    else:
        msgs.update(status='Q')
        print "-- kannel didn't accept these %s messages, we leave them queued -- " % msgs.count()

#        except Exception as e:
#            print "-- there was Error, %s messages are left queued -- " % msgs.count()
#            msgs.update(status='Q')
            
def send_all(router_url, to_send):
        pks = []
        if len(to_send):
            backend_name = to_send[0].connection.backend.name
            print "-- initializing sending messages for %s backend -- " % backend_name 
            for msg in to_send:
                if backend_name != msg.connection.backend.name:
                    # send all of the same backend
                    send_backend_chunk(router_url, pks, backend_name)
                    # reset the loop status variables to build the next chunk of messages with the same backend
                    backend_name = msg.connection.backend.name
                    pks = [msg.pk]
                else:
                    pks.append(msg.pk)
            print "-- sending out %s messages as a chunk through %s backend -- " % (len(pks), backend_name)
            send_backend_chunk(router_url, pks, backend_name)

def send_individual(router_url, backend):
    to_process = Message.objects.filter(direction='O', connection__backend__name=backend, status__in=['Q']).order_by('priority', 'status')
    print "-- processing %s individually -- " % to_process.count()
    if len(to_process):
        send_all(router_url, [to_process[0]])
                    
@task(track_started=True)
def send_kannel_messages_task():
    """
    Send MT messages to Kannel for onward forwarding to 
    """
#    import ipdb;ipdb.set_trace()
    print "-- starting the passing of messages to kannel --" 
    from .models import Message, MessageBatch
    backends = settings.KANNEL_BACKENDS
    CHUNK_SIZE = getattr(settings, 'MESSAGE_CHUNK_SIZE', 400)
#    while (True): #Removed since this is now run as a celery task
    for backend, router_url in backends.items():
        print "-- processing messages for %s backend -- " % backend 
        try:
            to_process = MessageBatch.objects.filter(status='Q')
            if to_process.count():
                print "-- %s batches found -- " % to_process.count() 
                batch = to_process[0]
                print "-- processing batch %s -- " % batch 
                to_process = batch.messages.filter(direction='O', connection__backend__name=backend, status__in=['Q']).order_by('priority', 'status')[:CHUNK_SIZE]
                if to_process.count():
                    print "-- batch %s contains %s messages to process -- " % (batch, to_process.count()) 
                    send_all(router_url, to_process)
                elif batch.messages.filter(status__in=['S', 'C']).count() == batch.messages.count():
                    print "-- batch %s has no more messages to processing, clossing it out -- " % batch 
                    batch.status = 'S'
                    batch.save()
                else:
                    print "-- sending individual message -- "  
                    send_individual(router_url, backend)
            else:
                print "-- sending individual message -- " 
                send_individual(router_url, backend)
        except Exception, exc:
            pass
