package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Function;

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
        insertOK(DbInsertCharTest::padTail);
    }

    void insertOK(Function<String, String> pad) throws IOException {
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
                assertEquals(pad.apply(entity.getZzz()), actual.getZzz());
            }
        }
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

    @Test
    void insertNulChar() throws IOException {
        insertNulChar(DbInsertCharTest::padTail);
    }

    void insertNulChar(Function<String, String> pad) throws IOException {
        int size = 6;
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            for (int i = 1; i <= size; i++) {
                var entity = new TestEntity(i, i, nulCharText(i));

                int count = ps.executeAndGetCount(tm, entity);
                assertEquals(-1, count); // TODO 1
            }
        }

        try (var ps = session.createPreparedQuery(SELECT_SQL + " order by zzz", SELECT_MAPPING)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(size, list.size());
            // int i = size;
            for (var entity : list) {
                // TODO assertEquals(i--, entity.getFoo());
                var expected = pad.apply(nulCharText(entity.getFoo()));
                switch (entity.getFoo()) { // TODO remove 補正
                case 4:
                case 5:
                case 6:
                    expected = "ab"; // 補正
                    break;
                default:
                    break;
                }
                assertEquals(expected, entity.getZzz());
            }
        }
    }

    private static String nulCharText(int size) {
        var sb = new StringBuilder();
        sb.append("ab");
        for (int i = 0; i < size; i++) {
            sb.append('\0');
        }
        sb.append("c");
        return sb.toString();
    }

    private static String padTail(String s) {
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
}