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
package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

class TgTxOptionRtxTest extends TgTxOptionTester {

    public TgTxOptionRtxTest() {
        super(TransactionType.READ_ONLY);
    }

    @Test
    void isRTX() {
        var txOption = TgTxOption.ofRTX();
        assertFalse(txOption.isOCC());
        assertFalse(txOption.isLTX());
        assertTrue(txOption.isRTX());
    }

    @Test
    void of() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX();
        String expected = "RTX{}";
        assertOption(expected, null, null, txOption);
    }

    @Test
    void ofTxOptionOcc() {
        TgTxOptionOcc srcTxOption = TgTxOption.ofOCC().label("abc");
        TgTxOptionRtx txOption = TgTxOption.ofRTX(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertNotEquals(srcTxOption, txOption);

        String expected = "RTX{label=abc}";
        assertOption(expected, "abc", null, txOption);
    }

    @Test
    void ofTxOptionLtx() {
        TgTxOptionLtx srcTxOption = TgTxOption.ofLTX("test").label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx txOption = TgTxOption.ofRTX(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertNotEquals(srcTxOption, txOption);

        String expected = "RTX{label=abc, priority=INTERRUPT}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, txOption);
    }

    @Test
    void ofTxOptionRtx() {
        TgTxOptionRtx srcTxOption = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx txOption = TgTxOption.ofRTX(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertEquals(srcTxOption.hashCode(), txOption.hashCode());
        assertEquals(srcTxOption, txOption);

        String expected = "RTX{label=abc, priority=INTERRUPT}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, txOption);
    }

    @Test
    void label() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc");
        String expected = "RTX{label=abc}";
        assertOption(expected, "abc", null, txOption);
    }

    @Test
    void priority() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "RTX{priority=DEFAULT}";
        assertOption(expected, null, TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, txOption);
    }

    @Test
    void clone0() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx clone = txOption.clone();
        assertNotSame(txOption, clone);
        assertEquals(txOption.hashCode(), clone.hashCode());
        assertEquals(txOption, clone);

        txOption.label(null);
        txOption.priority(null);
        assertOption("RTX{}", null, null, txOption);
        assertNotEquals(txOption, clone);

        String expected = "RTX{label=abc, priority=INTERRUPT}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx clone = txOption.clone("def");

        assertOption("RTX{label=abc, priority=INTERRUPT}", "abc", TransactionPriority.INTERRUPT, txOption);

        String expected = "RTX{label=def, priority=INTERRUPT}";
        assertOption(expected, "def", TransactionPriority.INTERRUPT, clone);
    }

    @Test
    void as() {
        TgTxOption txOption = TgTxOption.ofRTX();
        assertThrows(ClassCastException.class, () -> txOption.as(TgTxOptionOcc.class));
        assertThrows(ClassCastException.class, () -> txOption.as(TgTxOptionLtx.class));
        TgTxOptionRtx cast = txOption.as(TgTxOptionRtx.class);
        assertSame(txOption, cast);
    }

    @Test
    void asOccOption() {
        TgTxOption txOption = TgTxOption.ofRTX();
        assertThrows(ClassCastException.class, () -> txOption.asOccOption());
    }

    @Test
    void asLtxOption() {
        TgTxOption txOption = TgTxOption.ofRTX();
        assertThrows(ClassCastException.class, () -> txOption.asLtxOption());
    }

    @Test
    void asRtxOption() {
        TgTxOption txOption = TgTxOption.ofRTX();
        TgTxOptionRtx cast = txOption.asRtxOption();
        assertSame(txOption, cast);
    }

    private void assertOption(String text, String label, TransactionPriority priority, TgTxOptionRtx txOption) {
        assertEquals(text, txOption.toString());
        assertEquals(expectedType, txOption.type());
        assertEquals(label, txOption.label());
        assertEquals(priority, txOption.priority());

        assertLowOption(label, priority, false, List.of(), List.of(), List.of(), txOption);
    }
}
