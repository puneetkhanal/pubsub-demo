package demo;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class OrderedMessageRedelivery {

  public static final String TOPIC = "demo";
  public static final String SUBSCRIPTION = "demo-test-sub";
  public static final String MESSAGE_PREFIX = "message_";
  private static final Logger logger = LoggerFactory.getLogger(PubSubUtils.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    PubSubUtils.deleteSubscription(TOPIC, SUBSCRIPTION);
    PubSubUtils.deleteTopic(TOPIC);

    PubSubUtils.createTopic(TOPIC);
    PubSubUtils.createSubscription(TOPIC, null, SUBSCRIPTION);
    AtomicInteger messageIndex = new AtomicInteger(0);

    MessageReceiver receiver = (msg, ack) -> {
      logger.info("received message {}", msg.getData());
      if (messageIndex.get() > 0) {
        ack.ack();
      } else {
        //ack.nack(); // msg 0 is not ack/nack, expecting it to be redelivered, along with 1 and 2
      }
      messageIndex.incrementAndGet();
    };

    Subscriber subscriber = PubSubUtils.createSubscriber(SUBSCRIPTION, receiver);

    Publisher producer = PubSubUtils.createProducer(TOPIC);

    IntStream.range(0, 3).forEach(i -> {
      String messageText = MESSAGE_PREFIX + i;
      PubsubMessage message = PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(messageText))
          .setOrderingKey("A")
          .build();
      producer.publish(message);
    });

    subscriber.startAsync();

    Thread.sleep(600 * 1000 * 1000);
  }
}
