package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

class TgTxOptionOccTest extends TgTxOptionTester {

    public TgTxOptionOccTest() {
        super(TransactionType.SHORT);
    }

    @Test
    void empty() {
        TgTxOptionOcc option = TgTxOption.ofOCC();
        String expected = "OCC{}";
        assertOption(expected, null, null, //
                option);
    }

    @Test
    void label() {
        TgTxOptionOcc option = TgTxOption.ofOCC().label("abc");
        String expected = "OCC{label=abc}";
        assertOption(expected, "abc", null, //
                option);
    }

    @Test
    void priority() {
        TgTxOptionOcc option = TgTxOption.ofOCC().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "OCC{priority=DEFAULT}";
        assertOption(expected, null, TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, //
                option);
    }

    @Test
    void clone0() {
        TgTxOptionOcc option = TgTxOption.ofOCC().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionOcc clone = option.clone();

        option.label(null);
        option.priority(null);
        assertOption("OCC{}", null, null, option);

        String expected = "OCC{label=abc, priority=INTERRUPT}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, //
                clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionOcc option = TgTxOption.ofOCC().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionOcc clone = option.clone("def");

        assertOption("OCC{label=abc, priority=INTERRUPT}", "abc", TransactionPriority.INTERRUPT, //
                option);

        String expected = "OCC{label=def, priority=INTERRUPT}";
        assertOption(expected, "def", TransactionPriority.INTERRUPT, //
                clone);
    }

    private void assertOption(String text, String label, TransactionPriority priority, TgTxOptionOcc option) {
        assertEquals(text, option.toString());
        assertEquals(expectedType, option.type());
        assertEquals(label, option.label());
        assertEquals(priority, option.priority());

        assertLowOption(label, priority, List.of(), option);
    }
}
