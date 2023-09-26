package com.tsurugidb.iceaxe.test.insert;

import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * insert varchar test
 */
class DbInsertVarcharTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @Test
    void insertNull() throws Exception {
        new DbInsertCharTest().insertNull();
    }

    @Test
    void insertOK() throws Exception {
        new DbInsertCharTest().insertOK(Function.identity());
    }

    @Test
    void insertError() throws Exception {
        new DbInsertCharTest().insertError();
    }

    @Test
    void insertNulChar() throws Exception {
        new DbInsertCharTest().insertNulChar(Function.identity());
    }
}
