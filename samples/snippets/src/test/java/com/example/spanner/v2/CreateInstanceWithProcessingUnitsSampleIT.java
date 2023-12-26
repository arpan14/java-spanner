/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.spanner.v2;

import static com.google.common.truth.Truth.assertThat;

import com.example.spanner.SampleRunner;
import com.google.spanner.admin.database.v1.InstanceName;
import org.junit.Test;

public class CreateInstanceWithProcessingUnitsSampleIT extends SampleTestBaseV2 {

  @Test
  public void testCreateInstanceWithProcessingUnits() throws Exception {
    String instanceId = idGenerator.generateInstanceId();
    String out =
        SampleRunner.runSample(
            () -> CreateInstanceWithProcessingUnitsExample.createInstance(projectId, instanceId));
    assertThat(out)
        .contains(String.format("Instance %s has %d processing units",
            InstanceName.of(projectId, instanceId).toString(), 500));
  }
}
