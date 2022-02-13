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

package ab;

import ab.ntv.Station;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.ConnectionFactory;
import java.util.UUID;

public class Nt {
  public static void main(String[] args) throws Exception {
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

    for (int i = 0; i < 3; i++) {
      new Station(UUID.randomUUID().toString(), connectionFactory);
    }
    Thread.sleep(60_000);
  }
}
