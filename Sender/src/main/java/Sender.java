import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class Sender {

  private static final String EXCHANGE_NAME = "messages";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    try (Connection connection = factory.newConnection();
         Channel channel = connection.createChannel()) {
      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

      String severity = getRoutingKey(argv);
      while (true) {
        String message = getMessage();

        channel.basicPublish(EXCHANGE_NAME, "default", null, message.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        Thread.sleep(3000);
      }
    }
  }

  private static String getRoutingKey(String[] strings) {
    if (strings.length < 1)
      return "default";
    return strings[0];
  }

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