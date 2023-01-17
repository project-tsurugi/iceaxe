package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

class TgTxOptionLtxTest extends TgTxOptionTester {

    public TgTxOptionLtxTest() {
        super(TransactionType.LONG);
    }

    @Test
    void empty() {
        TgTxOptionLtx option = TgTxOption.ofLTX();
        String expected = "LTX{writePreserve=[]}";
        assertOption(expected, null, null, List.of(), //
                option);
    }

    @Test
    void writePreserveArray() {
        TgTxOptionLtx option = TgTxOption.ofLTX("t1", "t2");
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), //
                option);
    }

    @Test
    void writePreserveCollection() {
        TgTxOptionLtx option = TgTxOption.ofLTX(List.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), //
                option);
    }

    @Test
    void writePreserveStream() {
        TgTxOptionLtx option = TgTxOption.ofLTX(Stream.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), //
                option);
    }

    @Test
    void label() {
        TgTxOptionLtx option = TgTxOption.ofLTX().label("abc");
        String expected = "LTX{label=abc, writePreserve=[]}";
        assertOption(expected, "abc", null, List.of(), //
                option);
    }

    @Test
    void priority() {
        TgTxOptionLtx option = TgTxOption.ofLTX().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "LTX{priority=DEFAULT, writePreserve=[]}";
        assertOption(expected, null, TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, List.of(), //
                option);
    }

    @Test
    void clone0() {
        TgTxOptionLtx option = TgTxOption.ofLTX("t1").label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionLtx clone = option.clone();

        option.label(null);
        option.priority(null);
        option.addWritePreserve("t2");
        assertOption("LTX{writePreserve=[t1, t2]}", null, null, List.of("t1", "t2"), option);

        String expected = "LTX{label=abc, priority=INTERRUPT, writePreserve=[t1]}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, List.of("t1"), //
                clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionLtx option = TgTxOption.ofLTX("t1").label("abc");
        TgTxOptionLtx clone = option.clone("def");

        assertOption("LTX{label=abc, writePreserve=[t1]}", "abc", null, List.of("t1"), //
                option);

        String expected = "LTX{label=def, writePreserve=[t1]}";
        assertOption(expected, "def", null, List.of("t1"), //
                clone);
    }

    private void assertOption(String text, String label, TransactionPriority priority, List<String> writePreserve, TgTxOptionLtx option) {
        assertEquals(text, option.toString());
        assertEquals(expectedType, option.type());
        assertEquals(label, option.label());
        assertEquals(priority, option.priority());
        assertEquals(writePreserve, option.writePreserve());

        assertLowOption(label, priority, writePreserve, option);
    }
}
