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
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.api.response.ActivatedJob;
import io.zeebe.client.api.worker.*;

public abstract class AbstractService implements JobHandler {

  protected final AppCfg appCfg;
  protected final ZeebeClient client;

  protected AbstractService(AppCfg appCfg) {
    this.appCfg = appCfg;
    this.client =
        ZeebeClient.newClientBuilder()
            .brokerContactPoint(appCfg.getBrokerUrl())
            .numJobWorkerExecutionThreads(getNumThread())
            .withProperties(System.getProperties())
            .build();
  }

  public JobWorker create() {
    return this.client.newWorker().jobType(getJobType()).handler(this).open();
  }

  public abstract Integer getNumThread();

  public abstract String getJobType();

  @Override
  public abstract void handle(JobClient client, ActivatedJob job) throws Exception;
}
