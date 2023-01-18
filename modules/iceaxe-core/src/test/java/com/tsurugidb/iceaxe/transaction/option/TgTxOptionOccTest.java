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
        TgTxOptionOcc option = TgTxOption.ofOCC();
        String expected = "OCC{}";
        assertOption(expected, null, option);
    }

    @Test
    void label() {
        TgTxOptionOcc option = TgTxOption.ofOCC().label("abc");
        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", option);
    }

    @Test
    void clone0() {
        TgTxOptionOcc option = TgTxOption.ofOCC().label("abc");
        TgTxOptionOcc clone = option.clone();

        option.label(null);
        assertOption("OCC{}", null, option);

        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionOcc option = TgTxOption.ofOCC().label("abc");
        TgTxOptionOcc clone = option.clone("def");

        assertOption("OCC{label=abc}", "abc", option);

        String expected = "OCC{label=def}";
        assertOption(expected, "def", clone);
    }

    private void assertOption(String text, String label, TgTxOptionOcc option) {
        assertEquals(text, option.toString());
        assertEquals(expectedType, option.type());
        assertEquals(label, option.label());

        assertLowOption(label, null, List.of(), option);
    }
}
