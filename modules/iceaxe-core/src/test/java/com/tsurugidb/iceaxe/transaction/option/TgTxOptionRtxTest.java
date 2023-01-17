package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

class TgTxOptionRtxTest extends TgTxOptionTester {

    public TgTxOptionRtxTest() {
        super(TransactionType.READ_ONLY);
    }

    @Test
    void empty() {
        TgTxOptionRtx option = TgTxOption.ofRTX();
        String expected = "RTX{}";
        assertOption(expected, null, null, //
                option);
    }

    @Test
    void label() {
        TgTxOptionRtx option = TgTxOption.ofRTX().label("abc");
        String expected = "RTX{label=abc}";
        assertOption(expected, "abc", null, //
                option);
    }

    @Test
    void priority() {
        TgTxOptionRtx option = TgTxOption.ofRTX().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "RTX{priority=DEFAULT}";
        assertOption(expected, null, TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, //
                option);
    }

    @Test
    void clone0() {
        TgTxOptionRtx option = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx clone = option.clone();

        option.label(null);
        option.priority(null);
        assertOption("RTX{}", null, null, option);

        String expected = "RTX{label=abc, priority=INTERRUPT}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, //
                clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionRtx option = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx clone = option.clone("def");

        assertOption("RTX{label=abc, priority=INTERRUPT}", "abc", TransactionPriority.INTERRUPT, //
                option);

        String expected = "RTX{label=def, priority=INTERRUPT}";
        assertOption(expected, "def", TransactionPriority.INTERRUPT, //
                clone);
    }

    private void assertOption(String text, String label, TransactionPriority priority, TgTxOptionRtx option) {
        assertEquals(text, option.toString());
        assertEquals(expectedType, option.type());
        assertEquals(label, option.label());
        assertEquals(priority, option.priority());

        assertLowOption(label, priority, List.of(), option);
    }
}
