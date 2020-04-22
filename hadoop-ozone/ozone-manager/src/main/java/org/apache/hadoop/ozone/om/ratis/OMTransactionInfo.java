/*
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

package org.apache.hadoop.ozone.om.ratis;

import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.OMMetadataManager;

import java.io.IOException;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_SPLIT_KEY;

/**
 * TransactionInfo which is applied to OM DB.
 */
public class OMTransactionInfo {

  private long currentTerm;
  private long transactionIndex;

  public OMTransactionInfo(String transactionInfo) {

    // If no transactions are committed to DB, then transactionInfo will be
    // null. So return default values.
    if (transactionInfo == null) {
      currentTerm = -1;
      transactionIndex = 0;
    } else {
      String[] tInfo =
          transactionInfo.split(TRANSACTION_INFO_SPLIT_KEY);
      Preconditions.checkState(tInfo.length == 2, "Incorrect TransactionInfo " +
          "value");

      currentTerm = Long.parseLong(tInfo[0]);
      transactionIndex = Long.parseLong(tInfo[1]);
    }
  }

  /**
   * Get current term.
   * @return currentTerm
   */
  public long getCurrentTerm() {
    return currentTerm;
  }

  /**
   * Get current transaction index.
   * @return transactionIndex
   */
  public long getTransactionIndex() {
    return transactionIndex;
  }

  /**
   * Generate transaction info which need to be persisted in OM DB.
   * @param currentTerm
   * @param transactionIndex
   * @return transaction info.
   */
  public static String generateTransactionInfo(long currentTerm,
      long transactionIndex) {
    return currentTerm + TRANSACTION_INFO_SPLIT_KEY + transactionIndex;
  }

  public static OMTransactionInfo readTransactionInfo(
      OMMetadataManager metadataManager) throws IOException {
    String tInfo =
        metadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);
    return new OMTransactionInfo(tInfo);
  }
}
