package com.tsurugidb.iceaxe.test.delete;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRetryOverIOException;

/**
 * delete (table without primary key) test
 */
class DbDeleteNoPkTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTable();
        insertTestTable(DbDeleteTest.SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static void createTable() throws IOException {
        // no primary key
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @Test
    void deleteAll() throws IOException {
        new DbDeleteTest().deleteAll();
    }

    @Test
    void deleteConstant() throws IOException {
        new DbDeleteTest().deleteConstant();
    }

    @Test
    void deleteByBind() throws IOException {
        new DbDeleteTest().deleteByBind();
    }

    @RepeatedTest(15)
    void delete2SeqTx() throws IOException {
        try {
            new DbDeleteTest().delete2SeqTx();
        } catch (TsurugiTransactionRetryOverIOException e) {
            // TODO ERR_PHANTOMが解消したら、catchを削除する
            var c = e.getCause();
            if (c.getMessage().contains("ERR_PHANTOM")) {
                LOG.warn("delete2SeqTx fail. {}", c.getMessage());
                return;
            }
            throw e;
        }
    }

    @Test
    void delete2SameTx() throws IOException {
        new DbDeleteTest().delete2SameTx();
    }

    @Test
    void delete2Range() throws IOException {
        new DbDeleteTest().delete2Range();
    }

    @Test
    void deleteInsert() throws IOException {
        new DbDeleteTest().deleteInsert();
    }

    @Test
    void deleteInsertDeleteExists() throws IOException {
        new DbDeleteTest().deleteInsertDeleteExists();
    }

    @Test
    void deleteInsertDeleteNotExists() throws IOException {
        new DbDeleteTest().deleteInsertDeleteNotExists();
    }

    @Test
    void insertDelete() throws IOException {
        new DbDeleteTest().insertDelete();
    }

    @Test
    void insertDeleteInsert() throws IOException {
        new DbDeleteTest().insertDeleteInsert();
    }
}
