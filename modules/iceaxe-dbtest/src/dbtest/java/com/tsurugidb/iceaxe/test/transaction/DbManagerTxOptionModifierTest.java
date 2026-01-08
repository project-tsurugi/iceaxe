package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.exception.CcException;

class DbManagerTxOptionModifierTest extends DbTestTableTester {

    @Test
    void setFunction() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC().label("old"));
        tm.setTransactionOptionModifier(txOption -> txOption.clone("new"));
        tm.execute(transaction -> {
            assertEquals("new", transaction.getTransactionOption().label());
        });
    }

    @Test
    void setFunctionNull() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC().label("old"));
        tm.setTransactionOptionModifier((Function<TgTxOption, TgTxOption>) null);
        tm.execute(transaction -> {
            assertEquals("old", transaction.getTransactionOption().label());
        });
    }

    @Test
    void returnNull() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC().label("old"));
        tm.setTransactionOptionModifier(txOption -> null);
        var e = assertThrowsExactly(IllegalStateException.class, () -> {
            tm.execute(transaction -> {
            });
        });
        assertEquals("newTxOption is null", e.getMessage());
    }

    @Test
    void setTsurugiTmTxOptionModifier() throws Exception {
        var session = getSession();
        var setting = TgTmSetting.of(TgTxOption.ofOCC().label("label1"), 3);
        var tm = session.createTransactionManager(setting);
        tm.setTransactionOptionModifier((txOption, attempt) -> txOption.clone(txOption.label() + "-" + attempt));
        tm.execute(transaction -> {
            switch (transaction.getAttempt()) {
            case 0:
                assertEquals("label1-0", transaction.getTransactionOption().label());
                throw new TsurugiTransactionException(new CcException(SqlServiceCode.CC_EXCEPTION));
            case 1:
                assertEquals("label1-1", transaction.getTransactionOption().label());
                break;
            default:
                fail("tx=" + transaction);
                break;
            }
        });
    }
}
