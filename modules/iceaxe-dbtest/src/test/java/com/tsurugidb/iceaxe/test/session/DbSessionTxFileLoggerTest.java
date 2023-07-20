package com.tsurugidb.iceaxe.test.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogConfig;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiSessionTxFileLogger} test
 */
class DbSessionTxFileLoggerTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void testLog1() throws Exception {
        execute(TsurugiSessionTxFileLogConfig.EXPLAIN_BOTH, false);
    }

    @Test
    void testLog1Exception() throws Exception {
        execute(TsurugiSessionTxFileLogConfig.EXPLAIN_BOTH, true);
    }

    @Test
    void testLog2() throws Exception {
        execute(TsurugiSessionTxFileLogConfig.EXPLAIN_NOTHING, false);
    }

    @Test
    void testLog2Exception() throws Exception {
        execute(TsurugiSessionTxFileLogConfig.EXPLAIN_NOTHING, true);
    }

    private static final String ERROR_MESSAGE = "exception by test";

    private void execute(int writeExplain, boolean throwException) throws IOException, InterruptedException {
        var logDir = Files.createTempDirectory("iceaxe-dbtest.tx-log.");
        LOG.debug("logDir={}", logDir);

        var config = TsurugiSessionTxFileLogConfig.of(logDir).writeExplain(writeExplain);
        try (var session = DbTestConnector.createSession()) {
            session.addEventListener(new TsurugiSessionTxFileLogger(config));

            var foo = TgBindVariable.ofInt("foo");

            var tm = session.createTransactionManager(TgTxOption.ofOCC());
            tm.executeDdl(CREATE_TEST_SQL);
            try (var ps1 = session.createStatement(INSERT_SQL, INSERT_MAPPING); //
                    var ps2 = session.createQuery(SELECT_SQL, SELECT_MAPPING); //
                    var ps3 = session.createQuery(SELECT_SQL + " where foo=" + foo, TgParameterMapping.of(foo), SELECT_MAPPING); //
                    var ps4 = session.createStatement("delete from " + TEST)) {
                tm.execute(transaction -> {
                    var entity = createTestEntity(1);
                    transaction.executeAndGetCount(ps1, entity);

                    var list = transaction.executeAndGetList(ps2);
                    assertEquals(1, list.size());
                    assertEquals(entity, list.get(0));

                    var record = transaction.executeAndFindRecord(ps3, TgBindParameters.of(foo.bind(entity.getFoo()))).get();
                    assertEquals(entity, record);

                    transaction.executeAndGetCount(ps4);

                    if (throwException) {
                        throw new RuntimeException(ERROR_MESSAGE);
                    }
                });
            } catch (RuntimeException e) {
                if (throwException) {
                    assertEquals(ERROR_MESSAGE, e.getMessage());
                } else {
                    throw e;
                }
            }

            assertLog(logDir, writeExplain, throwException);
        } finally {
            deleteDir(logDir);
        }
    }

    private void assertLog(Path logDir, int writeExplain, boolean throwException) throws IOException {
        var list = listFiles(logDir);
        assertEquals(2, list.size()); // createのトランザクションとDMLのトランザクションの2つ

        {
            var list1 = listFiles(list.get(0));
            int logCount = 0;
            int explainCount = 0;
            for (var path : list1) {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".log")) {
                    logCount++;
                    String log1 = Files.readString(path, StandardCharsets.UTF_8);
                    assertContains(CREATE_TEST_SQL, log1);
                } else if (fileName.endsWith(".json")) {
                    explainCount++;
                } else {
                    fail(path.toString());
                }
            }

            assertEquals(1, logCount);
            if ((writeExplain & TsurugiSessionTxFileLogConfig.EXPLAIN_FILE) != 0) {
                assertEquals(0, explainCount); // TODO explainCount==1
            } else {
                assertEquals(0, explainCount);
            }
        }
        {
            var list2 = listFiles(list.get(1));
            int logCount = 0;
            int explainCount = 0;
            for (var path : list2) {
                String fileName = path.getFileName().toString();
                if (fileName.endsWith(".log")) {
                    logCount++;
                    String log2 = Files.readString(path, StandardCharsets.UTF_8);
                    assertContains(INSERT_SQL, log2);
                    assertContains(SELECT_SQL, log2);
                    if (throwException) {
                        assertContains("rollback end", log2);
                        assertContains(ERROR_MESSAGE, log2);
                    } else {
                        assertContains("commit end", log2);
                    }
                } else if (fileName.endsWith(".json")) {
                    explainCount++;
                } else {
                    fail(path.toString());
                }
            }

            assertEquals(1, logCount);
            if ((writeExplain & TsurugiSessionTxFileLogConfig.EXPLAIN_FILE) != 0) {
                assertEquals(2, explainCount); // TODO explainCount==4
            } else {
                assertEquals(0, explainCount);
            }
        }
    }

    private static List<Path> listFiles(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            var list = stream.collect(Collectors.toList());
            Collections.sort(list);
            return list;
        }
    }

    private void deleteDir(Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (Exception e) {
            LOG.warn("delete temp-dir error", e);
        }
    }
}
