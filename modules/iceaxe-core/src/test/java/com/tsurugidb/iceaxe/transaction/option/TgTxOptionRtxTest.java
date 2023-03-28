package com.tsurugidb.iceaxe.transaction.option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        assertOption(expected, null, null, List.of(), List.of(), //
                txOption);
    }

    @Test
    void label() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc");
        String expected = "RTX{label=abc}";
        assertOption(expected, "abc", null, List.of(), List.of(), //
                txOption);
    }

    @Test
    void priority() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().priority(TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED);
        String expected = "RTX{priority=DEFAULT}";
        assertOption(expected, null, TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED, List.of(), List.of(), //
                txOption);
    }

    @Test
    void inclusiveReadArea() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().addInclusiveReadArea("t1");
        String expected = "RTX{inclusiveReadArea=[t1]}";
        assertOption(expected, null, null, List.of("t1"), List.of(), //
                txOption);
    }

    @Test
    void inclusiveReadAreaArray() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().addInclusiveReadArea("t1", "t2");
        String expected = "RTX{inclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), //
                txOption);
    }

    @Test
    void inclusiveReadAreaCollection() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().addInclusiveReadArea(List.of("t1", "t2"));
        String expected = "RTX{inclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of("t1", "t2"), List.of(), //
                txOption);
    }

    @Test
    void exclusiveReadArea() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().addExclusiveReadArea("t1");
        String expected = "RTX{exclusiveReadArea=[t1]}";
        assertOption(expected, null, null, List.of(), List.of("t1"), //
                txOption);
    }

    @Test
    void exclusiveReadAreaArray() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().addExclusiveReadArea("t1", "t2");
        String expected = "RTX{exclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void exclusiveReadAreaCollection() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().addExclusiveReadArea(List.of("t1", "t2"));
        String expected = "RTX{exclusiveReadArea=[t1, t2]}";
        assertOption(expected, null, null, List.of(), List.of("t1", "t2"), //
                txOption);
    }

    @Test
    void clone0() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT).addInclusiveReadArea("in1").addExclusiveReadArea("ex1");
        TgTxOptionRtx clone = txOption.clone();

        txOption.label(null);
        txOption.priority(null);
        txOption.addInclusiveReadArea("in2");
        txOption.addExclusiveReadArea("ex2");
        assertOption("RTX{inclusiveReadArea=[in1, in2], exclusiveReadArea=[ex1, ex2]}", null, null, List.of("in1", "in2"), List.of("ex1", "ex2"), //
                txOption);

        String expected = "RTX{label=abc, priority=INTERRUPT, inclusiveReadArea=[in1], exclusiveReadArea=[ex1]}";
        assertOption(expected, "abc", TransactionPriority.INTERRUPT, List.of("in1"), List.of("ex1"), //
                clone);
    }

    @Test
    void cloneLabel() {
        TgTxOptionRtx txOption = TgTxOption.ofRTX().label("abc").priority(TransactionPriority.INTERRUPT);
        TgTxOptionRtx clone = txOption.clone("def");

        assertOption("RTX{label=abc, priority=INTERRUPT}", "abc", TransactionPriority.INTERRUPT, List.of(), List.of(), //
                txOption);

        String expected = "RTX{label=def, priority=INTERRUPT}";
        assertOption(expected, "def", TransactionPriority.INTERRUPT, List.of(), List.of(), //
                clone);
    }

    private void assertOption(String text, String label, TransactionPriority priority, List<String> inclusiveReadArea, List<String> exclusiveReadArea, TgTxOptionRtx txOption) {
        assertEquals(text, txOption.toString());
        assertEquals(expectedType, txOption.type());
        assertEquals(label, txOption.label());
        assertEquals(priority, txOption.priority());
        assertEquals(inclusiveReadArea, txOption.inclusiveReadArea());
        assertEquals(exclusiveReadArea, txOption.exclusiveReadArea());

        assertLowOption(label, priority, List.of(), inclusiveReadArea, exclusiveReadArea, txOption);
    }
}
