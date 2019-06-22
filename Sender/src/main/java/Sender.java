import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * sends messages to the exchange
 */
public class Sender {

  private static final String EXCHANGE_NAME = "messages";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

      String severity = getRoutingKey(argv);
      /*
      infinitely generates random alphanumeric string
      and pushes it to the exchange
      then sleeps 3s
       */
      while (true) {
        String message = getMessage();

        channel.basicPublish(EXCHANGE_NAME, "default", null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        Thread.sleep(3000);
      }
    }
  }

  /**
   * Provides routing key name from args if any or default
   *
   * @param strings if strings[0] exists then it will be returned
   * @return routingKey name
   */
  private static String getRoutingKey(String[] strings) {
    if (strings.length < 1)
      return "default";
    return strings[0];
  }

  /**
   * Generates random alphanumeric string with length 10
   *
   * @return alphanumeric string with length 10
   */
  private static String getMessage() {
    final String alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    final int N = alphabet.length();

    Random r = new Random();
    StringBuilder stringBuilder = new StringBuilder();

    for (int i = 0; i < 10; i++) {
      stringBuilder.append(alphabet.charAt(r.nextInt(N)));
    }

    return stringBuilder.toString();
  }

}