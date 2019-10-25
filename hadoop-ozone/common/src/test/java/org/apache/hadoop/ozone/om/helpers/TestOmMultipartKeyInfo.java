/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.UUID;

/**
 * Class to test OmMultipartKeyInfo.
 */
public class TestOmMultipartKeyInfo {

  @Test
  public void testCopyObject() {
    OmMultipartKeyInfo omMultipartKeyInfo = new OmMultipartKeyInfo(
        UUID.randomUUID().toString(), Time.now(),
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.THREE,
        new HashMap<>());

    OmMultipartKeyInfo cloneMultipartKeyInfo = omMultipartKeyInfo.copyObject();

    Assert.assertEquals(cloneMultipartKeyInfo, omMultipartKeyInfo);

    omMultipartKeyInfo.addPartKeyInfo(1, PartKeyInfo.newBuilder()
            .setPartKeyInfo(KeyInfo.newBuilder().getDefaultInstanceForType())
        .build());

    Assert.assertEquals(0, cloneMultipartKeyInfo.getPartKeyInfoMap().size());
    Assert.assertEquals(1, omMultipartKeyInfo.getPartKeyInfoMap().size());

  }
}
