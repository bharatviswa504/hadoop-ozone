/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.junit.Test;

import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_BLOCK_AND_CONTAINER_DB_HAS_METADATA;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_CHUNK;
import static org.junit.Assert.assertEquals;

/**
 * This class tests ChunkLayOutVersion.
 */
public class TestChunkLayOutVersion {

  @Test
  public void testVersionCount() {
    assertEquals(4, ChunkLayOutVersion.getAllVersions().size());
  }

  @Test
  public void testV1() {
    assertEquals(1, FILE_PER_CHUNK.getVersion());
  }


  @Test
  public void testV2() {
    assertEquals(2, FILE_PER_BLOCK.getVersion());
  }

  @Test
  public void testV3() {
    assertEquals(3,
        FILE_PER_BLOCK_AND_CONTAINER_DB_HAS_METADATA.getVersion());
  }

  @Test
  public void testV4() {
    assertEquals(4,
        FILE_PER_BLOCK_AND_CONTAINER_DB_HAS_METADATA.getVersion());
  }

}
