/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.iceaxe.transaction.manager.option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOptionOccLtx.TgTmTxOptionOccLtxExecuteInfo;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryStandardCode;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

class TgTmTxOptionOccLtxTest {

    private final TgTmRetryInstruction retry = TgTmRetryInstruction.of(TgTmRetryStandardCode.RETRYABLE, "test");
    private final TgTmRetryInstruction retryLtx = TgTmRetryInstruction.of(TgTmRetryStandardCode.RETRYABLE_LTX, "test");

    @Test
    void computeFirstTmOption() {
        var target = TgTmTxOptionOccLtx.of(TgTxOption.ofOCC(), 3, TgTxOption.ofLTX(), 2);
        var executeInfo = (TgTmTxOptionOccLtxExecuteInfo) target.createExecuteInfo(0);

        assertEqualsOcc(target.computeFirstTmOption(executeInfo).getTransactionOption());
        assertTrue(executeInfo.isOcc);
        assertEquals(1, executeInfo.occCounter);
    }

    @Test
    void computeRetryTmOption() {
        var target = TgTmTxOptionOccLtx.of(TgTxOption.ofOCC(), 3, TgTxOption.ofLTX(), 2);
        var executeInfo = (TgTmTxOptionOccLtxExecuteInfo) target.createExecuteInfo(0);

        int a = 0;
        assertEqualsOcc(target.computeFirstTmOption(executeInfo).getTransactionOption());
        assertEqualsOcc(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertEqualsOcc(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertTrue(target.computeRetryTmOption(executeInfo, ++a, null, retry).isRetryOver());
    }

    @Test
    void computeRetryTmOptionLtx1() {
        var target = TgTmTxOptionOccLtx.of(TgTxOption.ofOCC(), 3, TgTxOption.ofLTX(), 2);
        var executeInfo = (TgTmTxOptionOccLtxExecuteInfo) target.createExecuteInfo(0);

        int a = 0;
        assertEqualsOcc(target.computeFirstTmOption(executeInfo).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retryLtx).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertTrue(target.computeRetryTmOption(executeInfo, ++a, null, retry).isRetryOver());
    }

    @Test
    void computeRetryTmOptionLtx2() {
        var target = TgTmTxOptionOccLtx.of(TgTxOption.ofOCC(), 3, TgTxOption.ofLTX(), 2);
        var executeInfo = (TgTmTxOptionOccLtxExecuteInfo) target.createExecuteInfo(0);

        int a = 0;
        assertEqualsOcc(target.computeFirstTmOption(executeInfo).getTransactionOption());
        assertEqualsOcc(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retryLtx).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertTrue(target.computeRetryTmOption(executeInfo, ++a, null, retry).isRetryOver());
    }

    @Test
    void computeRetryTmOptionLtx3() {
        var target = TgTmTxOptionOccLtx.of(TgTxOption.ofOCC(), 3, TgTxOption.ofLTX(), 2);
        var executeInfo = (TgTmTxOptionOccLtxExecuteInfo) target.createExecuteInfo(0);

        int a = 0;
        assertEqualsOcc(target.computeFirstTmOption(executeInfo).getTransactionOption());
        assertEqualsOcc(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertEqualsOcc(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retryLtx).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(executeInfo, ++a, null, retry).getTransactionOption());
        assertTrue(target.computeRetryTmOption(executeInfo, ++a, null, retry).isRetryOver());
    }

    private static void assertEqualsOcc(TgTxOption actual) {
        assertEquals("OCC", actual.typeName());
    }

    private static void assertEqualsLtx(TgTxOption actual) {
        assertEquals("LTX", actual.typeName());
    }
}
