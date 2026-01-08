/*
 * Copyright 2023-2026 Project Tsurugi.
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

class TgTmTxOptionTest {

    @Test
    void execute() {
        var txOption = TgTxOption.ofOCC().label("test-label");
        var instruction = TgTmRetryInstruction.ofRetryable("test-reason");
        var target = TgTmTxOption.execute(txOption, instruction);

        assertTrue(target.isExecute());
        assertFalse(target.isRetryOver());
        assertSame(txOption, target.getTransactionOption());
        assertSame(instruction, target.getRetryInstruction());
        assertEquals(txOption + "(" + instruction + ")", target.toString());
    }

    @Test
    void retryOver() {
        var instruction = TgTmRetryInstruction.ofRetryable("test-reason");
        var target = TgTmTxOption.retryOver(instruction);

        assertFalse(target.isExecute());
        assertTrue(target.isRetryOver());
        assertNull(target.getTransactionOption());
        assertSame(instruction, target.getRetryInstruction());
        assertEquals(instruction.toString(), target.toString());
    }

    @Test
    void notRetryable() {
        var instruction = TgTmRetryInstruction.ofRetryable("test-reason");
        var target = TgTmTxOption.notRetryable(instruction);

        assertFalse(target.isExecute());
        assertFalse(target.isRetryOver());
        assertNull(target.getTransactionOption());
        assertSame(instruction, target.getRetryInstruction());
        assertEquals(instruction.toString(), target.toString());
    }
}
