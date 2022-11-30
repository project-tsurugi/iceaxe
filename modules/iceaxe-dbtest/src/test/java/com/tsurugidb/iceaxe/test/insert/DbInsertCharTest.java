package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert char test
 */
class DbInsertCharTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static void createTable() throws IOException {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int primary key," //
                + "  bar bigint," //
                + "  zzz char(" + ZZZ_SIZE + ")" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @Test
    void insertNull() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var entity = new TestEntity(1, 1, null);
            int count = ps.executeAndGetCount(tm, entity);
            assertEquals(-1, count); // TODO 1

            var actual = selectFromTest(entity.getFoo());
            assertEquals(entity, actual);
        }
    }

    @Test
    void insertOK() throws IOException {
        var list = List.of("", "0123456789", "あいう", "あいう0", "\0\1\uffff");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            int i = 0;
            for (var zzz : list) {
                assert zzz.getBytes(StandardCharsets.UTF_8).length <= ZZZ_SIZE;

                var entity = new TestEntity(++i, i, zzz);
                int count = ps.executeAndGetCount(tm, entity);
                assertEquals(-1, count); // TODO 1

                var actual = selectFromTest(entity.getFoo());
                assertEquals(entity.getFoo(), actual.getFoo());
                assertEquals(entity.getBar(), actual.getBar());
                assertEquals(fillTail(entity.getZzz()), actual.getZzz());
            }
        }
    }

    private static String fillTail(String s) {
        int length = s.getBytes(StandardCharsets.UTF_8).length;
        if (length >= ZZZ_SIZE) {
            return s;
        }
        var sb = new StringBuilder(s);
        for (int i = 0; i < ZZZ_SIZE - length; i++) {
            sb.append(" ");
        }
        return sb.toString();
    }

    @Test
    void insertError() throws IOException {
        // UTF-8でのバイト数がcharのサイズを超えるとエラー
        var list = List.of("0123456789A", "あいうえ", "あいう01");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            int i = 0;
            for (var zzz : list) {
                assert zzz.getBytes(StandardCharsets.UTF_8).length > ZZZ_SIZE;

                var entity = new TestEntity(++i, i, zzz);
                var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                    ps.executeAndGetCount(tm, entity);
                });
                assertEqualsCode(SqlServiceCode.ERR_TYPE_MISMATCH, e);
                assertContains("SQL--0019: .", e.getMessage()); // TODO エラー詳細情報の確認
            }
        }
    }
}
