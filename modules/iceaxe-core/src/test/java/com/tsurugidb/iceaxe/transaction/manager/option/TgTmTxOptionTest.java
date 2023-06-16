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
