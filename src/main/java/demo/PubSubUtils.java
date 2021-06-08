package demo;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.RetryPolicy;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PubSubUtils {

  private static final Logger logger = LoggerFactory.getLogger(PubSubUtils.class);
  private static final String PROJECT = "solr-dev";

  public static TransportChannelProvider getChannelProvider() {
    ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:8085").usePlaintext().build();
    return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
  }

  public static CredentialsProvider getCredentialsProvider() {
    return NoCredentialsProvider.create();
  }

  public static boolean isEmulatorMode() {
    return System.getenv("GOOGLE_APPLICATION_CREDENTIALS") == null;
  }

  public static Subscriber createSubscriber(String subscription, MessageReceiver receiver) {
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT, subscription);

    FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
        .setMaxOutstandingElementCount(10000L)
        .setLimitExceededBehavior(LimitExceededBehavior.ThrowException)
        .build();

    Subscriber subscriber = getSubscriber(subscriptionName, receiver, flowControlSettings);
    return subscriber;
  }

  public static void createSubscription(String topicName, String deadLetterTopicName, String subscription) throws IOException {
    try (SubscriptionAdminClient subscriptionAdminClient = PubSubUtils.getSubscriptionAdminClient()) {
      ProjectTopicName projectTopicName = ProjectTopicName.of(PROJECT, topicName);
      //ProjectTopicName projectDeadLetterTopicName = ProjectTopicName.of(PROJECT, topicName);

      /*DeadLetterPolicy deadLetterPolicy =
          DeadLetterPolicy.newBuilder()
              .setDeadLetterTopic(projectDeadLetterTopicName.toString())
              .setMaxDeliveryAttempts(10)
              .build();*/

      ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT, subscription);

      Subscription request = Subscription.newBuilder()
          .setName(subscriptionName.toString())
          .setTopic(projectTopicName.toString())
          .setEnableMessageOrdering(true)
          //.setRetryPolicy(RetryPolicy.newBuilder()
            //  .setMinimumBackoff(Duration.newBuilder().setSeconds(5).build())
              //.setMaximumBackoff(Duration.newBuilder().setSeconds(600).build())
              //.build())
          .setAckDeadlineSeconds(15)
          .build();

      try {
        subscriptionAdminClient.createSubscription(request);
      } catch (AlreadyExistsException e) {
        logger.info("subscription already exists: {}", subscription);
      }

    }
  }

  public static void deleteSubscription(String name, String subscription) throws IOException {
    try (SubscriptionAdminClient subscriptionAdminClient = PubSubUtils.getSubscriptionAdminClient()) {
      ProjectTopicName topicName = ProjectTopicName.of(PROJECT, name);

      ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT, subscription);

      try {
        subscriptionAdminClient.deleteSubscription(subscriptionName);
      } catch (NotFoundException e) {
        logger.info("subscription not found: {}", subscription);
      }

    }
  }

  public static Publisher createProducer(String name) throws IOException {
    TopicName topicName = TopicName.of(PROJECT, name);
    return getPublisher(topicName);
  }

  public static void createTopic(String name) {
    TopicName topicName = TopicName.of(PROJECT, name);
    try (TopicAdminClient topicAdminClient = PubSubUtils.getTopicAdminClient()) {
      topicAdminClient.createTopic(topicName);
    } catch (AlreadyExistsException | IOException e) {
      logger.info("topic already exists: {}", name);
    }
  }

  public static void deleteTopic(String name) throws IOException {
    TopicName topicName = TopicName.of(PROJECT, name);
    try (TopicAdminClient topicAdminClient = PubSubUtils.getTopicAdminClient()) {
      topicAdminClient.deleteTopic(topicName);
    } catch (NotFoundException e) {
      logger.info("topic not found: {}", name);
    }
  }

  public static TopicAdminClient getTopicAdminClient() throws IOException {
    return isEmulatorMode() ? TopicAdminClient.create(
        TopicAdminSettings.newBuilder()
            .setTransportChannelProvider(getChannelProvider())
            .setCredentialsProvider(getCredentialsProvider()).build())
        : TopicAdminClient.create();
  }

  public static SubscriptionAdminClient getSubscriptionAdminClient() throws IOException {

    return isEmulatorMode() ? SubscriptionAdminClient.create(
        SubscriptionAdminSettings.newBuilder()
            .setTransportChannelProvider(getChannelProvider())
            .setCredentialsProvider(getCredentialsProvider())
            .build())
        : SubscriptionAdminClient.create();
  }

  public static Publisher getPublisher(TopicName topicName) throws IOException {
    return PubSubUtils.isEmulatorMode() ?
        Publisher.newBuilder(topicName)
            .setChannelProvider(PubSubUtils.getChannelProvider())
            .setCredentialsProvider(PubSubUtils.getCredentialsProvider())
            .setEnableMessageOrdering(true)
            .build() :
        Publisher.newBuilder(topicName)
            .setEnableMessageOrdering(true)
            .build();
  }

  public static Subscriber getSubscriber(ProjectSubscriptionName subscriptionName,
                                         MessageReceiver receiver,
                                         FlowControlSettings flowControlSettings) {

    return PubSubUtils.isEmulatorMode() ?
        Subscriber.newBuilder(subscriptionName, receiver)
            .setChannelProvider(PubSubUtils.getChannelProvider())
            .setCredentialsProvider(PubSubUtils.getCredentialsProvider())
            .setParallelPullCount(5)
            .setMaxAckExtensionPeriod(org.threeten.bp.Duration.ofSeconds(30))
            .setFlowControlSettings(flowControlSettings)
            .build() :
        Subscriber.newBuilder(subscriptionName, receiver)
            .setParallelPullCount(5)
            .setFlowControlSettings(flowControlSettings)
            .setMaxAckExtensionPeriod(org.threeten.bp.Duration.ofSeconds(30))
            .build();
  }

  public static void shutDownProducer(Publisher docProducer) {
    docProducer.shutdown();
    try {
      docProducer.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
