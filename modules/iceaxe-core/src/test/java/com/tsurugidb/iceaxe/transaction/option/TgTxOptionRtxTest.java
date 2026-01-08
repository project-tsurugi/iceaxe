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
package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.OptionalInt;

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
        new RtxOptionAssert().text(expected).check(txOption);
    }

    @Test
    void ofTxOptionOcc() {
        TgTxOptionOcc srcTxOption = TgTxOption.ofOCC().label("abc");
        TgTxOptionRtx txOption = TgTxOption.ofRTX(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertNotEquals(srcTxOption, txOption);

        String expected = "RTX{label=abc}";
        new RtxOptionAssert().text(expected).label("abc").check(txOption);
    }

    @Test
    void ofTxOptionLtx() {
        TgTxOptionLtx srcTxOption = TgTxOption.ofLTX("test").label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx txOption = TgTxOption.ofRTX(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertNotEquals(srcTxOption, txOption);

        String expected = "RTX{label=abc, priority=INTERRUPT}";
        new RtxOptionAssert().text(expected).label("abc").priority(TransactionPriority.INTERRUPT).check(txOption);
    }

    @Test
    void ofTxOptionRtx() {
        TgTxOptionRtx srcTxOption = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx txOption = TgTxOption.ofRTX(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertEquals(srcTxOption.hashCode(), txOption.hashCode());
        assertEquals(srcTxOption, txOption);

        String expected = "RTX{label=abc, priority=INTERRUPT}";
        new RtxOptionAssert().text(expected).label("abc").priority(TransactionPriority.INTERRUPT).check(txOption);
    }

    @Test
    void label() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc");
        String expected = "RTX{label=abc}";
        new RtxOptionAssert().text(expected).label("abc").check(txOption);
    }

    @Test
    void scanParallel() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().scanParallel(123);
        String expected = "RTX{scanParallel=123}";
        new RtxOptionAssert().text(expected).scanParallel(123).check(txOption);
    }

    @Test
    void priority() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "RTX{priority=DEFAULT}";
        new RtxOptionAssert().text(expected).priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED).check(txOption);
    }

    @Test
    void clone0() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc").scanParallel(123).priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx clone = txOption.clone();
        assertNotSame(txOption, clone);
        assertEquals(txOption.hashCode(), clone.hashCode());
        assertEquals(txOption, clone);

        txOption.label(null);
        txOption.priority(null);
        txOption.scanParallel(0);
        new RtxOptionAssert().text("RTX{scanParallel=0}").scanParallel(0).check(txOption);
        assertNotEquals(txOption, clone);

        String expected = "RTX{label=abc, priority=INTERRUPT, scanParallel=123}";
        new RtxOptionAssert().text(expected).label("abc").scanParallel(123).priority(TransactionPriority.INTERRUPT).check(clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc").scanParallel(123).priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx clone = txOption.clone("def");

        new RtxOptionAssert().text("RTX{label=abc, priority=INTERRUPT, scanParallel=123}").label("abc").scanParallel(123).priority(TransactionPriority.INTERRUPT).check(txOption);

        String expected = "RTX{label=def, priority=INTERRUPT, scanParallel=123}";
        new RtxOptionAssert().text(expected).label("def").scanParallel(123).priority(TransactionPriority.INTERRUPT).check(clone);
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

    private class RtxOptionAssert {
        private String text;
        private String label;
        private TransactionPriority priority;
        private Integer scanParallel;

        public RtxOptionAssert text(String text) {
            this.text = text;
            return this;
        }

        public RtxOptionAssert label(String label) {
            this.label = label;
            return this;
        }

        public RtxOptionAssert priority(TransactionPriority priority) {
            this.priority = priority;
            return this;
        }

        public RtxOptionAssert scanParallel(int scanParallel) {
            this.scanParallel = scanParallel;
            return this;
        }

        public void check(TgTxOptionRtx txOption) {
            assertEquals(text, txOption.toString());
            assertEquals(expectedType, txOption.type());
            assertEquals(label, txOption.label());
            assertEquals(priority, txOption.priority());
            if (scanParallel != null) {
                assertEquals(OptionalInt.of(scanParallel), txOption.scanParallel());
            } else {
                assertTrue(txOption.scanParallel().isEmpty());
            }

            assertLowOption(label, priority, false, List.of(), List.of(), List.of(), scanParallel, txOption);
        }
    }
}
