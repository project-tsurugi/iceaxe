package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void isLTX() {
        var txOption = TgTxOption.ofLTX();
        assertFalse(txOption.isOCC());
        assertTrue(txOption.isLTX());
        assertFalse(txOption.isRTX());
    }

    @Test
    void of() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX();
        String expected = "LTX{writePreserve=[]}";
        assertOption(expected, null, null, List.of(), List.of(), List.of(), //
                txOption);
    }

    @Test
    void ofArray() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX("t1", "t2");
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), List.of(), //
                txOption);
    }

    @Test
    void ofCollection() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX(List.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), List.of(), //
                txOption);
    }

    @Test
    void ofStream() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX(Stream.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), List.of(), //
                txOption);
    }

    @Test
    void label() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().label("abc");
        String expected = "LTX{label=abc, writePreserve=[]}";
        assertOption(expected, "abc", null, List.of(), List.of(), List.of(), //
                txOption);
    }

    @Test
    void priority() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "LTX{priority=DEFAULT, writePreserve=[]}";
        assertOption(expected, null, TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, List.of(), List.of(), List.of(), //
                txOption);
    }

    @Test
    void writePreserve() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addWritePreserve("t1");
        String expected = "LTX{writePreserve=[t1]}";
        assertOption(expected, null, null, List.of("t1"), List.of(), List.of(), //
                txOption);
    }

    @Test
    void writePreserveArray() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addWritePreserve("t1", "t2");
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), List.of(), //
                txOption);
    }

    @Test
    void writePreserveCollection() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addWritePreserve(List.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), List.of(), //
                txOption);
    }

    @Test
    void writePreserveStream() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addWritePreserve(Stream.of("t1", "t2"));
        String expected = "LTX{writePreserve=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), List.of(), //
                txOption);
    }

    @Test
    void inclusiveReadArea() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addInclusiveReadArea("t1");
        String expected = "LTX{writePreserve=[], inclusiveReadArea=[t1]}";
        assertOption(expected, null, null, List.of(), List.of("t1"), List.of(), //
                txOption);
    }

    @Test
    void inclusiveReadAreaArray() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addInclusiveReadArea("t1", "t2");
        String expected = "LTX{writePreserve=[], inclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of("t1", "t2"), List.of(), //
                txOption);
    }

    @Test
    void inclusiveReadAreaCollection() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addInclusiveReadArea(List.of("t1", "t2"));
        String expected = "LTX{writePreserve=[], inclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of("t1", "t2"), List.of(), //
                txOption);
    }

    @Test
    void inclusiveReadAreaStream() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addInclusiveReadArea(Stream.of("t1", "t2"));
        String expected = "LTX{writePreserve=[], inclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of("t1", "t2"), List.of(), //
                txOption);
    }

    @Test
    void exclusiveReadArea() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addExclusiveReadArea("t1");
        String expected = "LTX{writePreserve=[], exclusiveReadArea=[t1]}";
        assertOption(expected, null, null, List.of(), List.of(), List.of("t1"), //
                txOption);
    }

    @Test
    void exclusiveReadAreaArray() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addExclusiveReadArea("t1", "t2");
        String expected = "LTX{writePreserve=[], exclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of(), List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void exclusiveReadAreaCollection() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addExclusiveReadArea(List.of("t1", "t2"));
        String expected = "LTX{writePreserve=[], exclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of(), List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void exclusiveReadAreaStream() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX().addExclusiveReadArea(Stream.of("t1", "t2"));
        String expected = "LTX{writePreserve=[], exclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of(), List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void clone0() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX("t1").label("abc").priority(TransactionPriority.INTERRUPT).addInclusiveReadArea("in1").addExclusiveReadArea("ex1");
        TgTxOptionLtx clone = txOption.clone();

        txOption.label(null);
        txOption.priority(null);
        txOption.addWritePreserve("t2");
        txOption.addInclusiveReadArea("in2");
        txOption.addExclusiveReadArea("ex2");
        assertOption("LTX{writePreserve=[t1, t2], inclusiveReadArea=[in1, in2], exclusiveReadArea=[ex1, ex2]}", null, null, List.of("t1", "t2"), List.of("in1", "in2"), List.of("ex1", "ex2"), //
                txOption);

        String expected = "LTX{label=abc, priority=INTERRUPT, writePreserve=[t1], inclusiveReadArea=[in1], exclusiveReadArea=[ex1]}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, List.of("t1"), List.of("in1"), List.of("ex1"), //
                clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionLtx txOption = TgTxOption.ofLTX("t1").label("abc");
        TgTxOptionLtx clone = txOption.clone("def");

        assertOption("LTX{label=abc, writePreserve=[t1]}", "abc", null, List.of("t1"), List.of(), List.of(), //
                txOption);

        String expected = "LTX{label=def, writePreserve=[t1]}";
        assertOption(expected, "def", null, List.of("t1"), List.of(), List.of(), //
                clone);
    }

    private void assertOption(String text, String label, TransactionPriority priority, List<String> writePreserve, List<String> inclusiveReadArea, List<String> exclusiveReadArea,
            TgTxOptionLtx txOption) {
        assertEquals(text, txOption.toString());
        assertEquals(expectedType, txOption.type());
        assertEquals(label, txOption.label());
        assertEquals(priority, txOption.priority());
        assertEquals(writePreserve, txOption.writePreserve());
        assertEquals(inclusiveReadArea, txOption.inclusiveReadArea());
        assertEquals(exclusiveReadArea, txOption.exclusiveReadArea());

        assertLowOption(label, priority, writePreserve, inclusiveReadArea, exclusiveReadArea, txOption);
    }
}
