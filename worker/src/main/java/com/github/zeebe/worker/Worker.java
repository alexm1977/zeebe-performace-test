/*
 * Copyright © 2021 camunda services GmbH (info@camunda.com)
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
package com.github.zeebe.worker;

import com.github.zeebe.worker.config.AppCfg;
import com.github.zeebe.worker.services.*;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.worker.JobWorker;
import java.util.*;

public class Worker extends App {

  private final AppCfg appCfg;

  private static final List<JobWorker> workerList = new ArrayList<>();

  Worker(AppCfg appCfg) {
    this.appCfg = appCfg;
  }

  @Override
  public void run() {

    List<? extends AbstractService> jobTypeList =
        List.of(
            new Service_1(appCfg),
            new Service_2(appCfg),
            new Service_3(appCfg),
            new Service_4(appCfg));

    final ZeebeClient client = createZeebeClient();
    printTopology(client);

    jobTypeList.forEach(
        jobType -> {
          workerList.add(jobType.create());
        });

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  workerList.forEach(JobWorker::close);
                  client.close();
                }));
  }

  private ZeebeClient createZeebeClient() {
    return ZeebeClient.newClientBuilder()
        .brokerContactPoint(appCfg.getBrokerUrl())
        .numJobWorkerExecutionThreads(4)
        .withProperties(System.getProperties())
        .build();
  }

  public static void main(String[] args) {
    createApp(Worker::new);
  }
}
