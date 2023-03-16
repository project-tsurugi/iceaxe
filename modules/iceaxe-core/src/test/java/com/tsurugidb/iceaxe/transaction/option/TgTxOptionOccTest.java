package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

class TgTxOptionOccTest extends TgTxOptionTester {

    public TgTxOptionOccTest() {
        super(TransactionType.SHORT);
    }

    @Test
    void empty() {
        TgTxOptionOcc txOption = TgTxOption.ofOCC();
        String expected = "OCC{}";
        assertOption(expected, null, txOption);
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

        txOption.label(null);
        assertOption("OCC{}", null, txOption);

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

    private void assertOption(String text, String label, TgTxOptionOcc txOption) {
        assertEquals(text, txOption.toString());
        assertEquals(expectedType, txOption.type());
        assertEquals(label, txOption.label());

        assertLowOption(label, null, List.of(), txOption);
    }
}
