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
import io.zeebe.client.ZeebeClient;
import java.util.concurrent.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Starter extends App {

  private final AppCfg appCfg;

  Starter(AppCfg appCfg) {
    this.appCfg = appCfg;
  }

  @Override
  public void run() {
    final int rate = 50;
    final String processId = "perf_test";
    final ZeebeClient client = createZeebeClient();

    printTopology(client);

    final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    deployWorkflow(client, "bpmn/tst_perf.bpmn");

    // start instances
    final int intervalMs = Math.floorDiv(1000, rate);
    log.info("Creating an instance every {}ms", intervalMs);
    executorService.scheduleAtFixedRate(
        () -> {
          try {
            client.newCreateInstanceCommand().bpmnProcessId(processId).latestVersion().send();
          } catch (Exception e) {
            e.printStackTrace();
          }
        },
        0,
        intervalMs,
        TimeUnit.MILLISECONDS);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  executorService.shutdown();
                  try {
                    executorService.awaitTermination(60, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    e.printStackTrace();
                  }
                  client.close();
                }));
  }

  private ZeebeClient createZeebeClient() {
    return ZeebeClient.newClientBuilder()
        .brokerContactPoint(appCfg.getBrokerUrl())
        .numJobWorkerExecutionThreads(0)
        .withProperties(System.getProperties())
        .build();
  }

  private void deployWorkflow(ZeebeClient client, String bpmnXmlPath) {
    while (true) {
      try {
        client.newDeployCommand().addResourceFromClasspath(bpmnXmlPath).send().join();
        break;
      } catch (Exception e) {
        log.warn("Failed to deploy workflow, retrying", e);
        try {
          Thread.sleep(200);
        } catch (InterruptedException ex) {
          // ignore
        }
      }
    }
  }

  public static void main(String[] args) {
    createApp(Starter::new);
  }
}
