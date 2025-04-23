/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Collectors;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption.ScanParallelOptCase;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

public class TgTxOptionTester {

    protected final TransactionType expectedType;

    public TgTxOptionTester(TransactionType type) {
        this.expectedType = type;
    }

    protected void assertLowOption(String label, TransactionPriority priority, boolean includeDdl, List<String> writePreserve, List<String> inclusiveReadArea, List<String> exclusiveReadArea,
            TgTxOption txOption) {
        assertLowOption(label, priority, includeDdl, writePreserve, inclusiveReadArea, exclusiveReadArea, null, txOption);
    }

    protected void assertLowOption(String label, TransactionPriority priority, boolean includeDdl, List<String> writePreserve, List<String> inclusiveReadArea, List<String> exclusiveReadArea,
            Integer scanParallel, TgTxOption txOption) {
        var builder = txOption.toLowTransactionOption();
        assertEquals(expectedType, builder.getType());
        assertEquals((label != null) ? label : "", builder.getLabel());
        assertEquals((priority != null) ? priority : TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, builder.getPriority());
        assertEquals(includeDdl, builder.getModifiesDefinitions());
        assertEquals(writePreserve, builder.getWritePreservesList().stream().map(wp -> wp.getTableName()).collect(Collectors.toList()));
        assertEquals(inclusiveReadArea, builder.getInclusiveReadAreasList().stream().map(ra -> ra.getTableName()).collect(Collectors.toList()));
        assertEquals(exclusiveReadArea, builder.getExclusiveReadAreasList().stream().map(ra -> ra.getTableName()).collect(Collectors.toList()));
        if (scanParallel != null) {
            assertEquals(ScanParallelOptCase.SCAN_PARALLEL, builder.getScanParallelOptCase());
            assertEquals(scanParallel, builder.getScanParallel());
        } else {
            assertEquals(ScanParallelOptCase.SCANPARALLELOPT_NOT_SET, builder.getScanParallelOptCase());
            assertEquals(0, builder.getScanParallel());
        }
    }
}
