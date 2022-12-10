package com.tsurugidb.iceaxe.test.insert;

import java.io.IOException;
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
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void insertNull() throws IOException {
        new DbInsertCharTest().insertNull();
    }

    @Test
    void insertOK() throws IOException {
        new DbInsertCharTest().insertOK(Function.identity());
    }

    @Test
    void insertError() throws IOException {
        new DbInsertCharTest().insertError();
    }

    @Test
    void insertNulChar() throws IOException {
        new DbInsertCharTest().insertNulChar(Function.identity());
    }
}
