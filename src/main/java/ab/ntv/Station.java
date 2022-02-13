/*
 * Copyright 2022 Aleksei Balan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ab.ntv;

import javax.jms.*;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Station implements MessageListener {
  private final String callSign;
  private final ConnectionFactory connectionFactory;

  public Station(String callSign, ConnectionFactory connectionFactory) {
    this.callSign = callSign;
    this.connectionFactory = connectionFactory;
    new Thread(this::producer).start();
    new Thread(this::consumer).start();
  }

  public void producer() {
    Random random = ThreadLocalRandom.current();
    try {
      Connection connection = connectionFactory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createTopic("topic");
      MessageProducer producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      connection.start();
      while (true) {
        Thread.sleep(random.nextInt(2000) + 1000);
        String message = UUID.randomUUID().toString();
        System.out.println(callSign + " -> " + message);
        producer.send(session.createTextMessage(message));
      }
    } catch (JMSException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void consumer() {
    try {
      Connection connection = connectionFactory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createTopic("topic");
      MessageConsumer consumer = session.createConsumer(destination);
      connection.start();
      //consumer.setMessageListener(this);
      while (true) {
        Message message = consumer.receive();
        System.out.println(callSign + " <- " + ((TextMessage) message).getText());
      }
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onMessage(Message message) {
    try {
      System.out.println(callSign + " <- " + ((TextMessage) message).getText());
    } catch (JMSException e) {
      e.printStackTrace();
    }
  }
}
