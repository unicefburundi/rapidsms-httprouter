import datetime
import django

from django.db import models, connections, transaction
from django.db.models.query import QuerySet

from rapidsms.models import Contact, Connection

mass_text_sent = django.dispatch.Signal(providing_args=["messages", "status"])

DIRECTION_CHOICES = (
    ('I', "Incoming"),
    ('O', "Outgoing"))

STATUS_CHOICES = (
    ('R', "Received"),
    ('H', "Handled"),

    ('P', "Processing"),
    ('L', "Locked"),

    ('Q', "Queued"),
    ('S', "Sent"),
    ('D', "Delivered"),

    ('C', "Cancelled"),
    ('E', "Errored")
)

#Once we start mass_texting, batching messages becomes absolutely necessary
class MessageBatch(models.Model):
    status = models.CharField(max_length=1, choices=STATUS_CHOICES)
    name = models.CharField(max_length=15,null=True,blank=True)
    
    def __unicode__(self):
        return '%s %s --> %s' % (self.pk, self.name, self.status) 
    
class Message(models.Model):
    connection = models.ForeignKey(Connection, related_name='messages')
    text       = models.TextField()

    direction  = models.CharField(max_length=1, choices=DIRECTION_CHOICES)
    status     = models.CharField(max_length=1, choices=STATUS_CHOICES)

    date       = models.DateTimeField(auto_now_add=True)
    updated    = models.DateTimeField(auto_now=True, null=True)

    sent       = models.DateTimeField(null=True, blank=True)
    delivered  = models.DateTimeField(null=True, blank=True)

    batch = models.ForeignKey(MessageBatch, related_name='messages', null=True) #a message may belong to a batch
    priority = models.IntegerField(default=10, db_index=True) #messages in a batch may be prioritized
    
    in_response_to = models.ForeignKey('self', related_name='responses', null=True, blank=True)

    def __unicode__(self):
        # crop the text (to avoid exploding the admin)
        if len(self.text) < 60: str = self.text
        else: str = "%s..." % (self.text[0:57])

        to_from = (self.direction == "I") and "to" or "from"
        return "%s (%s %s)" % (str, to_from, self.connection.identity)

    def as_json(self):
        return dict(id=self.pk,
                    contact=self.connection.identity, backend=self.connection.backend.name,
                    direction=self.direction, status=self.status, text=self.text,
                    date=self.date.isoformat())

    def send(self):
        """
        Triggers our celery task to send this message off.  Note that our dependency to Celery
        is a soft one, as we only do the import of Tasks here.  If a user has ROUTER_URL
        set to NONE (say when using an Android relayer) then there is no need for Celery and friends.
        """
        from tasks import send_message_task

        # send this message off in celery
        send_message_task.delay(self.pk)
        
    #Some times its necessary to Mass insert messages
    @classmethod
    @transaction.commit_on_success
    def mass_text(cls, text, connections, status='P', batch_status='Q'):
        
        #imported here to make the dependency on celery soft
        from tasks import queue_messages_task
        msg_batch = MessageBatch.objects.create(status=batch_status)
        entries = []
        
        for connection in connections:
            entries += [Message(text=text, date=datetime.datetime.now(), direction='O', status=status, batch=msg_batch, connection=connection, priority=10)]
        Message.objects.bulk_create(entries)

        toret = Message.objects.order_by('-pk')[0:connections.count()]
        
        #respond to these messages by queuing them up in celery
#        queue_messages_task.delay(toret)
        mass_text_sent.send(sender=msg_batch, messages=toret, status=status)
        return toret

class DeliveryError(models.Model):
    """
    Simple class to keep track of delivery errors for messages.  We retry up to three times before
    finally giving up on sending.
    """
    message = models.ForeignKey(Message, related_name='errors',
                                help_text="The message that had an error")
    log = models.TextField(help_text="A short log on the error that was received when this message was delivered")
    created_on = models.DateTimeField(auto_now_add=True,
                                      help_text="When this delivery error occurred")

