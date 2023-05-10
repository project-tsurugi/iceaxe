package com.tsurugidb.iceaxe.transaction.manager.option;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

class TgTmTxOptionMultipleListTest {

    @Test
    void findTxOption1() {
        var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 3);

        assertEqualsOcc(target.findTxOption(0));
        assertEqualsOcc(target.findTxOption(1));
        assertEqualsOcc(target.findTxOption(2));
        assertNull(target.findTxOption(3));
    }

    @Test
    void findTxOption() {
        var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 3).add(TgTxOption.ofLTX(), 2);

        assertEqualsOcc(target.findTxOption(0));
        assertEqualsOcc(target.findTxOption(1));
        assertEqualsOcc(target.findTxOption(2));
        assertEqualsLtx(target.findTxOption(3));
        assertEqualsLtx(target.findTxOption(4));
        assertNull(target.findTxOption(5));
    }

    @Test
    void computeFirstTmOption() {
        var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 3).add(TgTxOption.ofLTX(), 2);

        assertEqualsOcc(target.computeFirstTmOption().getTransactionOption());
    }

    @Test
    void computeRetryTmOption() {
        var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 3).add(TgTxOption.ofLTX(), 2);

        assertEqualsOcc(target.computeRetryTmOption(1, null).getTransactionOption());
        assertEqualsOcc(target.computeRetryTmOption(2, null).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(3, null).getTransactionOption());
        assertEqualsLtx(target.computeRetryTmOption(4, null).getTransactionOption());
        assertTrue(target.computeRetryTmOption(5, null).isRetryOver());
    }

    @Test
    void limit() {
        var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 2).add(TgTxOption.ofLTX(), Integer.MAX_VALUE);

        assertEqualsOcc(target.findTxOption(0));
        assertEqualsOcc(target.findTxOption(1));
        assertEqualsLtx(target.findTxOption(2));
        assertEqualsLtx(target.findTxOption(Integer.MAX_VALUE - 1));
        assertNull(target.findTxOption(Integer.MAX_VALUE));
    }

    @Test
    void limit2() {
        var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), Integer.MAX_VALUE - 1).add(TgTxOption.ofLTX(), 1);

        assertEqualsOcc(target.findTxOption(0));
        assertEqualsOcc(target.findTxOption(Integer.MAX_VALUE - 2));
        assertEqualsLtx(target.findTxOption(Integer.MAX_VALUE - 1));
        assertNull(target.findTxOption(Integer.MAX_VALUE));
    }

    @Test
    void limitOver() {
        {
            var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), Integer.MAX_VALUE);
            assertThrows(IllegalArgumentException.class, () -> {
                target.add(TgTxOption.ofLTX(), 1);
            });
        }
        {
            var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 2).add(TgTxOption.ofLTX(), Integer.MAX_VALUE);
            assertThrows(IllegalArgumentException.class, () -> {
                target.add(TgTxOption.ofLTX(), 1);
            });
        }
    }

    @Test
    void description() {
        {
            var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 3);
            assertEquals("OCC{}*3", target.getDescription());
        }
        {
            var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 3).add(TgTxOption.ofLTX(), 2);
            assertEquals("OCC{}*3, LTX{writePreserve=[]}*2", target.getDescription());
        }
        {
            var target = TgTmTxOptionMultipleList.of().add(TgTxOption.ofOCC(), 3).add(TgTxOption.ofRTX(), 2).add(TgTxOption.ofLTX(), 1);
            assertEquals("OCC{}*3, RTX{}*2, LTX{writePreserve=[]}*1", target.getDescription());
        }
    }

    private static void assertEqualsOcc(TgTxOption actual) {
        assertEquals("OCC", actual.typeName());
    }

    private static void assertEqualsLtx(TgTxOption actual) {
        assertEquals("LTX", actual.typeName());
    }
}
