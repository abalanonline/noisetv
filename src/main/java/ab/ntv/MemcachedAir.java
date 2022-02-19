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

import net.spy.memcached.MemcachedClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

public class MemcachedAir implements Atmosphere {
  public static final String KEY = "noisetv";
  public static final int EXP = 60;

  private static MemcachedClient newMemcachedClient() {
    // docker run -d --rm --name memcached -p 11211:11211 memcached:alpine
    // ssh -L 11211:memcached.cfg.cache.amazonaws.com:11211 ec2-123-45-67-89.ca-central-1.compute.amazonaws.com
    final InetSocketAddress inetSocketAddress = new InetSocketAddress("localhost", 11211);
    try {
      return new MemcachedClient(inetSocketAddress);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public Transmitter installTransmitter() {
    return new Tx(newMemcachedClient());
  }

  @Override
  public Receiver installReceiver() {
    return new Rx(newMemcachedClient());
  }

  public static class Tx implements Transmitter {
    private final MemcachedClient client;

    public Tx(MemcachedClient client) {
      this.client = client;
    }

    @Override
    public void send(String message) {
      long i = client.incr(KEY, 1, 0, EXP);
      client.set(KEY + i, EXP, message);
    }
  }

  public static class Rx implements Receiver, Runnable {
    private final MemcachedClient client;
    private Consumer<String> listener;
    private int recorded = Integer.MAX_VALUE;

    public Rx(MemcachedClient client) {
      this.client = client;
      new Thread(this).start(); // single threaded
    }

    @Override
    public String receive() {
      throw new IllegalStateException("no blocking");
    }

    private String getMessage(int i) {
      return (String) client.get(KEY + i);
    }

    private String getMessage() {
      String strLive = (String) client.get(KEY);
      if (strLive == null) {
        return null;
      }
      int live = Integer.parseInt(strLive);
      if (recorded > live) {
        recorded = live - 1;
      }
      if (recorded < live) {
        return getMessage(++recorded);
      }
      return null;
    }

    @Override
    public void run() {
      while (true) {
        String message = getMessage();
        if (message != null && listener != null) {
          listener.accept(message);
          continue;
        }
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          break;
        }
      }
    }

    @Override
    public void setListener(Consumer<String> listener) {
      this.listener = listener;
    }
  }
}
