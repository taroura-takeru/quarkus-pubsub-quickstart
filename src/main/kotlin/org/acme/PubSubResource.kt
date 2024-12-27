package org.acme

import com.google.api.core.ApiFutureCallback
import com.google.api.core.ApiFutures
//import com.google.api.gax.core.CredentialsProvider
//import com.google.api.gax.core.NoCredentialsProvider
//import com.google.api.gax.rpc.TransportChannelProvider
import com.google.cloud.pubsub.v1.*
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage
import io.quarkiverse.googlecloudservices.pubsub.QuarkusPubSub
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import jakarta.inject.Inject
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType
import org.jboss.logging.Logger
import java.io.IOException
import java.util.concurrent.TimeUnit


@Path("/pubsub")
class PubSubResource {
    @Inject
    private lateinit var pubSub: QuarkusPubSub

    private lateinit var subscriber: Subscriber

//    private var credentialsProvider: CredentialsProvider = NoCredentialsProvider()

    @PostConstruct
    @Throws(IOException::class)
    fun init() {
        // init topic and subscription
        pubSub.createTopic("test-topic")
        pubSub.createSubscription("test-topic", "test-subscription")

        // Subscribe to PubSub
        val receiver =
            MessageReceiver { message: PubsubMessage, consumer: AckReplyConsumer ->
                LOG.infov("Got message {0}", message.data.toStringUtf8())
                consumer.ack()
            }
        subscriber = pubSub.subscriber("test-subscription", receiver)
        subscriber.startAsync().awaitRunning()
    }

    @PreDestroy
    fun destroy() {
        // Stop the subscription at destroy time
        subscriber.stopAsync()
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Throws(IOException::class, InterruptedException::class)
    fun pubsub() {
        // Init a publisher to the topic
//        var publisher: Publisher = Publisher.newBuilder("test-topic")
//                .setCredentialsProvider(credentialsProvider)
//                // Set the ChannelProvider
//                .setChannelProvider(channelProvider).build()
        val publisher = pubSub.publisher("test-topic")
        try {
            val data = ByteString.copyFromUtf8("my-message")
            // Create a new message
            val pubsubMessage = PubsubMessage.newBuilder().setData(data).build()
            val messageIdFuture = publisher.publish(pubsubMessage) // Publish the message
            ApiFutures.addCallback(messageIdFuture, object :
                    ApiFutureCallback<String?> {
                    // Wait for message submission and log the result
                    override fun onSuccess(messageId: String?) {
                        LOG.infov("published with message id {0}", messageId)
                    }

                    override fun onFailure(t: Throwable) {
                        LOG.warnv("failed to publish: {0}", t)
                    }
                },
                MoreExecutors.directExecutor()
            )
        } finally {
            publisher.shutdown()
            publisher.awaitTermination(1, TimeUnit.MINUTES)
        }
    }

    companion object {
        private val LOG: Logger = Logger.getLogger(PubSubResource::class.java)
    }

//    @ConfigProperty(name = "quarkus.google.cloud.pubsub.emulator-host")
//    private lateinit var emulatorHost: String

//    private lateinit var channelProvider: TransportChannelProvider

    // Create a ChannelProvider that connects to the Dev Service
//    fun connectDevService(subscriptionName:String,receiver:MessageReceiver) {
//        val channel: ManagedChannel = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build()
//        channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
//        // Create a subscriber and set the ChannelProvider
//        subscriber = Subscriber.newBuilder(subscriptionName, receiver).setChannelProvider(channelProvider).build()
//        subscriber.startAsync().awaitRunning()
//    }

//    fun initSubscription() {
//        val subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
//                .setCredentialsProvider(credentialsProvider)
//                // Set the ChannelProvider
//                .setTransportChannelProvider(channelProvider).build()
//    }
}