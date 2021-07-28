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
package com.github.zeebe.worker.events;

import com.github.zeebe.worker.config.AppCfg;
import io.zeebe.client.ZeebeClient;

public class Message_2 extends AbstractEvent {

  public Message_2(AppCfg appCfg, ZeebeClient client) {
    super(appCfg, client);
  }

  @Override
  protected String getMessageName() {
    return "msg_2";
  }
}
