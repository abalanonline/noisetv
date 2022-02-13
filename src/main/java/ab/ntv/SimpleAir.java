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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class SimpleAir implements Atmosphere, Runnable {
  private final BlockingQueue<String> queue;
  private final Set<SimpleReceiver> receivers;

  public SimpleAir() {
    this.queue = new LinkedBlockingQueue<>();
    this.receivers = new HashSet<>();
    new Thread(this).start();
  }

  @Override
  public Transmitter installTransmitter() {
    return new SimpleTransmitter(queue);
  }

  @Override
  public Receiver installReceiver() {
    SimpleReceiver simpleReceiver = new SimpleReceiver();
    receivers.add(simpleReceiver);
    return simpleReceiver;
  }

  @Override
  public void run() {
    while (true) {
      try {
        String message = queue.take();
        receivers.forEach(receiver -> new Thread(() -> {
          if (receiver.listener != null) {
            receiver.listener.accept(message);
          } else {
            try {
              receiver.queue.put(message);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }).start());
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  public static class SimpleTransmitter implements Transmitter {
    private final BlockingQueue<String> queue;

    public SimpleTransmitter(BlockingQueue<String> queue) {
      this.queue = queue;
    }

    @Override
    public void send(String message) {
      new Thread(() -> {
        try {
          queue.put(message);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }).start();
    }
  }

  public static class SimpleReceiver implements Receiver {
    private final BlockingQueue<String> queue;
    private Consumer<String> listener;

    public SimpleReceiver() {
      this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public String receive() {
      if (listener != null) {
        throw new IllegalStateException("Cannot synchronously receive a message when a Listener is set");
      }
      try {
        return queue.take();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void setListener(Consumer<String> listener) {
      this.listener = listener;
    }
  }
}
