import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class Receiver {

  private static final String EXCHANGE_NAME = "messages";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
    String queueName = channel.queueDeclare().getQueue();

    if (argv.length < 1) {
      channel.queueBind(queueName, EXCHANGE_NAME, "default");
    } else {
      for (String routingKey : argv) {
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
      }
    }

    System.out.println(" [*] Waiting for messages.");

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
      System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
    };
    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
    });
  }
}