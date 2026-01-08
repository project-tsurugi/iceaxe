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

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

class TgTxOptionOccTest extends TgTxOptionTester {

    public TgTxOptionOccTest() {
        super(TransactionType.SHORT);
    }

    @Test
    void isOCC() {
        var txOption = TgTxOption.ofOCC();
        assertTrue(txOption.isOCC());
        assertFalse(txOption.isLTX());
        assertFalse(txOption.isRTX());
    }

    @Test
    void of() {
        TgTxOptionOcc txOption = TgTxOption.ofOCC();
        String expected = "OCC{}";
        assertOption(expected, null, txOption);
    }

    @Test
    void ofTxOptionOcc() {
        TgTxOptionOcc srcTxOption = TgTxOption.ofOCC().label("abc");
        TgTxOptionOcc txOption = TgTxOption.ofOCC(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertEquals(srcTxOption.hashCode(), txOption.hashCode());
        assertEquals(srcTxOption, txOption);

        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", txOption);
    }

    @Test
    void ofTxOptionLtx() {
        TgTxOptionLtx srcTxOption = TgTxOption.ofLTX("test").label("abc");
        TgTxOptionOcc txOption = TgTxOption.ofOCC(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertNotEquals(srcTxOption, txOption);

        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", txOption);
    }

    @Test
    void ofTxOptionRtx() {
        TgTxOptionRtx srcTxOption = TgTxOption.ofRTX().label("abc");
        TgTxOptionOcc txOption = TgTxOption.ofOCC(srcTxOption);
        assertNotSame(srcTxOption, txOption);
        assertNotEquals(srcTxOption, txOption);

        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", txOption);
    }

    @Test
    void label() {
        TgTxOptionOcc txOption = TgTxOption.ofOCC().label("abc");
        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", txOption);
    }

    @Test
    void clone0() {
        TgTxOptionOcc txOption = TgTxOption.ofOCC().label("abc");
        TgTxOptionOcc clone = txOption.clone();
        assertNotSame(txOption, clone);
        assertEquals(txOption.hashCode(), clone.hashCode());
        assertEquals(txOption, clone);

        txOption.label(null);
        assertOption("OCC{}", null, txOption);
        assertNotEquals(txOption, clone);

        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionOcc txOption = TgTxOption.ofOCC().label("abc");
        TgTxOptionOcc clone = txOption.clone("def");

        assertOption("OCC{label=abc}", "abc", txOption);

        String expected = "OCC{label=def}";
        assertOption(expected, "def", clone);
    }

    @Test
    void as() {
        TgTxOption txOption = TgTxOption.ofOCC();
        TgTxOptionOcc cast = txOption.as(TgTxOptionOcc.class);
        assertSame(txOption, cast);
        assertThrows(ClassCastException.class, () -> txOption.as(TgTxOptionLtx.class));
        assertThrows(ClassCastException.class, () -> txOption.as(TgTxOptionRtx.class));
    }

    @Test
    void asOccOption() {
        TgTxOption txOption = TgTxOption.ofOCC();
        TgTxOptionOcc cast = txOption.asOccOption();
        assertSame(txOption, cast);
    }

    @Test
    void asLtxOption() {
        TgTxOption txOption = TgTxOption.ofOCC();
        assertThrows(ClassCastException.class, () -> txOption.asLtxOption());
    }

    @Test
    void asRtxOption() {
        TgTxOption txOption = TgTxOption.ofOCC();
        assertThrows(ClassCastException.class, () -> txOption.asRtxOption());
    }

    private void assertOption(String text, String label, TgTxOptionOcc txOption) {
        assertEquals(text, txOption.toString());
        assertEquals(expectedType, txOption.type());
        assertEquals(label, txOption.label());

        assertLowOption(label, null, false, List.of(), List.of(), List.of(), txOption);
    }
}
