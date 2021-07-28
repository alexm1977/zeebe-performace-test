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
import com.github.zeebe.worker.events.*;
import io.zeebe.client.api.response.ActivatedJob;
import io.zeebe.client.api.worker.JobClient;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Service_1 extends AbstractService {

  static final long completionDelay = 300;

  private final List<? extends AbstractEvent> event_list;

  public Service_1(AppCfg appCfg) {
    super(appCfg);
    this.event_list =
        List.of(
            new Message_1(this.getAppCfg(), this.getClient()),
            new Message_2(this.getAppCfg(), this.getClient()));
  }

  @Override
  public void handle(JobClient jobClient, ActivatedJob job) throws Exception {
    try {
      Thread.sleep(completionDelay);
      ContextVariables context = job.getVariablesAsType(ContextVariables.class);
      generateEvent(context.getTask_id());
    } catch (Exception e) {
      e.printStackTrace();
    }

    jobClient.newCompleteCommand(job.getKey()).variables(job.getVariables()).send();
  }

  private void generateEvent(String correlationKey) {
    var index = Long.valueOf(Math.round(Math.random() * (event_list.size() - 1))).intValue();
    log.info("try send event id:{}", index);
    event_list.get(index).createEvent(correlationKey);
  }

  @Override
  public Integer getNumThread() {
    return 3;
  }

  @Override
  public String getJobType() {
    return "tst_service_1";
  }
}
