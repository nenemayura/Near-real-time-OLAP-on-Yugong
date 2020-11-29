package taskScheduler;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.BufferedReader;
import java.io.FileReader;

public class NewTask {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        try (BufferedReader br = new BufferedReader(new FileReader("/Users/gouthamreddykotapalle/Goo_Dumbo/Near-real-time-OLAP-on-Yugong/messages.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                // process the line
                channel.basicPublish("", TASK_QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        line.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + line + "'");
            }
        }
    }

}