package com.tsurugidb.iceaxe.test.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * transaction RTX test
 */
class DbTransactionRtxTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @RepeatedTest(10)
    void ltxUpdate_rtx() throws Exception {
        var session = getSession();

        try (var tx1 = session.createTransaction(TgTxOption.ofLTX(TEST))) {
            try (var updatePs = session.createStatement("update " + TEST + " set bar = 9")) {
                tx1.executeAndGetCount(updatePs);
//              Thread.sleep(100);

                try (var tx2 = session.createTransaction(TgTxOption.ofRTX())) {
                    try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                        var list = tx2.executeAndGetList(selectPs);
                        assertEqualsTestTable(SIZE, list);
                    }
                    tx2.commit(TgCommitType.DEFAULT);
                }
            }

            tx1.commit(TgCommitType.DEFAULT);
        }
    }

    @RepeatedTest(10)
    void ltxSelect_rtx() throws Exception {
        var session = getSession();

        try (var tx1 = session.createTransaction(TgTxOption.ofLTX())) {
            try (var select1Ps = session.createQuery("select * from " + TEST + " where foo=1")) {
                tx1.executeAndGetList(select1Ps);
//              Thread.sleep(100);

                try (var tx2 = session.createTransaction(TgTxOption.ofRTX())) {
                    try (var selectPs = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                        var list = tx2.executeAndGetList(selectPs);
                        assertEqualsTestTable(SIZE, list);
                    }
                    tx2.commit(TgCommitType.DEFAULT);
                }
            }

            tx1.commit(TgCommitType.DEFAULT);
        }
    }
}
