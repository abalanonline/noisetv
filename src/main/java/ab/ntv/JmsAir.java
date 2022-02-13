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
import java.util.function.Consumer;

public class JmsAir implements Atmosphere {
  private final ConnectionFactory connectionFactory;
  private final String topic;

  public JmsAir(ConnectionFactory connectionFactory, String topic) {
    this.connectionFactory = connectionFactory;
    this.topic = topic;
  }

  @Override
  public Transmitter installTransmitter() {
    try {
      Connection connection = connectionFactory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createTopic(topic);
      MessageProducer producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      connection.start();
      return new JmsTransmitter(producer, session);
    } catch (JMSException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Receiver installReceiver() {
    try {
      Connection connection = connectionFactory.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Destination destination = session.createTopic(topic);
      MessageConsumer consumer = session.createConsumer(destination);
      connection.start();
      return new JmsReceiver(consumer);
    } catch (JMSException e) {
      throw new RuntimeException(e);
    }
  }

  public static class JmsTransmitter implements Transmitter {
    private final MessageProducer producer;
    private final Session session;

    public JmsTransmitter(MessageProducer producer, Session session) {
      this.producer = producer;
      this.session = session;
    }

    @Override
    public void send(String message) {
      try {
        producer.send(session.createTextMessage(message));
      } catch (JMSException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class JmsReceiver implements Receiver {
    private final MessageConsumer consumer;

    public JmsReceiver(MessageConsumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public String receive() {
      try {
        return ((TextMessage) consumer.receive()).getText();
      } catch (JMSException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void setListener(Consumer<String> listener) {
      try {
        consumer.setMessageListener(message -> {
          try {
            listener.accept(((TextMessage) message).getText());
          } catch (JMSException e) {
            throw new RuntimeException(e);
          }
        });
      } catch (JMSException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
