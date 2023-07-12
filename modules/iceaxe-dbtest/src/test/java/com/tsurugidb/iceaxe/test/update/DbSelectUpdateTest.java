package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * 'select for update'-like test
 */
class DbSelectUpdateTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void selectUpdate_occ_occ() throws Exception {
        TgTxOption txOption1 = TgTxOption.ofOCC();
        TgTxOption txOption2 = TgTxOption.ofOCC();
        selectUpdate(txOption1, txOption2, true, false);
    }

    @Test
    void selectUpdate_occ_ltx() throws Exception {
        TgTxOption txOption1 = TgTxOption.ofOCC();
        TgTxOption txOption2 = TgTxOption.ofLTX(TEST);
        selectUpdate(txOption1, txOption2, true, false);
    }

    @Test
    void selectUpdate_ltx_occ() throws Exception {
        TgTxOption txOption1 = TgTxOption.ofLTX(TEST);
        TgTxOption txOption2 = TgTxOption.ofOCC();
        selectUpdate(txOption1, txOption2, false, true);
    }

    private void selectUpdate(TgTxOption txOption1, TgTxOption txOption2, boolean tx1Error, boolean tx2Error) throws Exception {
        int key = 2;
        var selectSql = SELECT_SQL + " where foo=" + key;
        var updateSql = "update " + TEST + " set bar=:bar where foo=" + key;
        var updateMapping = TgParameterMapping.ofSingle("bar", long.class);

        var session = getSession();

        try (var selectPs = session.createQuery(selectSql, SELECT_MAPPING); //
                var updatePs = session.createStatement(updateSql, updateMapping)) {
            try (var tx1 = session.createTransaction(txOption1.label("tx1"))) {
                var entity = tx1.executeAndGetList(selectPs).get(0);

                try (var tx2 = session.createTransaction(txOption2.label("tx2"))) {
                    if (tx2Error) {
                        var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                            tx2.executeAndGetCount(updatePs, 9L);
                            tx2.commit(TgCommitType.DEFAULT);
                        });
                        assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);
                        tx2.rollback();
                    } else {
                        tx2.executeAndGetCount(updatePs, 9L);
                        tx2.commit(TgCommitType.DEFAULT);
                    }
                }

                tx1.executeAndGetCount(updatePs, entity.getBar() + 1);
                if (tx1Error) {
                    var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                        tx1.commit(TgCommitType.DEFAULT);
                    });
                    assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);
                } else {
                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }
    }

    @Test
    void selectUpdate_ltx_ltx() throws Exception {
        TgTxOption txOption1 = TgTxOption.ofLTX(TEST);
        TgTxOption txOption2 = TgTxOption.ofLTX(TEST);

        int key = 2;
        var selectSql = SELECT_SQL + " where foo=" + key;
        var updateSql = "update " + TEST + " set bar=:bar where foo=" + key;
        var updateMapping = TgParameterMapping.ofSingle("bar", long.class);

        var session = getSession();

        try (var selectPs = session.createQuery(selectSql, SELECT_MAPPING); //
                var updatePs = session.createStatement(updateSql, updateMapping)) {
            try (var tx1 = session.createTransaction(txOption1.label("tx1"))) {
                var entity = tx1.executeAndGetList(selectPs).get(0);

                try (var tx2 = session.createTransaction(txOption2.label("tx2"))) {
                    tx2.executeAndGetCount(updatePs, 9L);
                    var future2 = executeFuture(() -> {
                        tx2.commit(TgCommitType.DEFAULT);
                        return null;
                    });

                    tx1.executeAndGetCount(updatePs, entity.getBar() + 1);
                    assertFalse(future2.isDone());
                    tx1.commit(TgCommitType.DEFAULT);

                    var e = assertThrows(ExecutionException.class, () -> future2.get());
                    var c = e.getCause();
                    assertInstanceOf(TsurugiTransactionException.class, c);
                    assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, c);
                }
            }
        }
    }
}
