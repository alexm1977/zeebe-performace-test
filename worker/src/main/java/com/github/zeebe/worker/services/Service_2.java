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
package com.github.zeebe.worker.services;

import com.github.zeebe.worker.config.AppCfg;
import io.zeebe.client.api.response.ActivatedJob;
import io.zeebe.client.api.worker.JobClient;

public final class Service_2 extends AbstractService {

  static final long completionDelay = 300;

  public Service_2(AppCfg appCfg) {
    super(appCfg);
  }

  @Override
  public Integer getNumThread() {
    return 3;
  }

  @Override
  public String getJobType() {
    return "tst_service_2";
  }

  @Override
  public void handle(JobClient jobClient, ActivatedJob job) throws Exception {
    try {
      Thread.sleep(completionDelay);
    } catch (Exception e) {
      e.printStackTrace();
    }

    jobClient.newCompleteCommand(job.getKey()).variables(job.getVariables()).send();
  }
}
