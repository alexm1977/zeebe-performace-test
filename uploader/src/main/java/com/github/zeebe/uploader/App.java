/**
 * Copyright © 2021 Aleksandr Mukhin (alex.omsk1977@gmail.com)
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
package com.github.zeebe.uploader;

import com.github.zeebe.uploader.config.AppCfg;
import com.typesafe.config.*;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.response.Topology;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
abstract class App implements Runnable {

  static void createApp(Function<AppCfg, Runnable> appFactory) {
    final Config config = ConfigFactory.load().getConfig("app");
    log.info("Starting app with config: {}", config.root().render());
    final AppCfg appCfg = ConfigBeanFactory.create(config, AppCfg.class);
    appFactory.apply(appCfg).run();
  }

  protected void printTopology(ZeebeClient client) {
    while (true) {
      try {
        final Topology topology = client.newTopologyRequest().send().join();
        topology
            .getBrokers()
            .forEach(
                b -> {
                  log.info("Broker {} - {}", b.getNodeId(), b.getAddress());
                  b.getPartitions()
                      .forEach(p -> log.info("{} - {}", p.getPartitionId(), p.getRole()));
                });
        break;
      } catch (Exception e) {
        // retry
      }
    }
  }
}
