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

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Station {
  private final String callSign;
  private final Atmosphere.Transmitter transmitter;
  private final Atmosphere.Receiver receiver;

  public Station(String callSign, Atmosphere atmosphere) {
    this.callSign = callSign;
    this.transmitter = atmosphere.installTransmitter();
    this.receiver = atmosphere.installReceiver();
    new Thread(this::producer).start();
    //new Thread(this::consumer).start();
    this.receiver.setListener(this::listener);
  }

  public void producer() {
    Random random = ThreadLocalRandom.current();
    while (true) {
      try {
        Thread.sleep(random.nextInt(2000) + 1000);
      } catch (InterruptedException e) {
        break;
      }
      String message = UUID.randomUUID().toString();
      System.out.println(callSign + " -> " + message);
      transmitter.send(message);
    }
  }

  public void consumer() {
    while (true) {
      listener(receiver.receive());
    }
  }

  public void listener(String message) {
    System.out.println(callSign + " <- " + message);
  }
}
