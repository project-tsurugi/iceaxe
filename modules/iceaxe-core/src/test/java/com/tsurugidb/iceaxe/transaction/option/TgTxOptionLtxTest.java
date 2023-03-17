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
        TgTxOptionLtx txOption = TgTxOption.ofLTX();
        String expected = "LTX{writePreserve=[]}";
        assertOption(expected, null, null, List.of(), //
                txOption);
    }

    @Test
    void writePreserveArray() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX("t1", "t2");
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void writePreserveCollection() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX(List.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void writePreserveStream() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX(Stream.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void label() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().label("abc");
        String expected = "LTX{label=abc, writePreserve=[]}";
        assertOption(expected, "abc", null, List.of(), //
                txOption);
    }

    @Test
    void priority() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "LTX{priority=DEFAULT, writePreserve=[]}";
        assertOption(expected, null, TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, List.of(), //
                txOption);
    }

    @Test
    void clone0() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX("t1").label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionLtx clone = txOption.clone();

        txOption.label(null);
        txOption.priority(null);
        txOption.addWritePreserve("t2");
        assertOption("LTX{writePreserve=[t1, t2]}", null, null, List.of("t1", "t2"), txOption);

        String expected = "LTX{label=abc, priority=INTERRUPT, writePreserve=[t1]}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, List.of("t1"), //
                clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX("t1").label("abc");
        TgTxOptionLtx clone = txOption.clone("def");

        assertOption("LTX{label=abc, writePreserve=[t1]}", "abc", null, List.of("t1"), //
                txOption);

        String expected = "LTX{label=def, writePreserve=[t1]}";
        assertOption(expected, "def", null, List.of("t1"), //
                clone);
    }

    private void assertOption(String text, String label, TransactionPriority priority, List<String> writePreserve, TgTxOptionLtx txOption) {
        assertEquals(text, txOption.toString());
        assertEquals(expectedType, txOption.type());
        assertEquals(label, txOption.label());
        assertEquals(priority, txOption.priority());
        assertEquals(writePreserve, txOption.writePreserve());

        assertLowOption(label, priority, writePreserve, txOption);
    }
}
