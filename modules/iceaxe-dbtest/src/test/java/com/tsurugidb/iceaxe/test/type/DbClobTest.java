package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.metadata.TgSqlColumn;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.mapping.TgSingleResultMapping;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.sql.type.TgClobReference;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeFileUtil;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;

/**
 * CLOB test
 */
class DbClobTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTable();

        logInitEnd(info);
    }

    private static void createTable() throws IOException, InterruptedException {
        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value clob" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, "INT", list.get(0));
        assertColumn("value", TgDataType.CLOB, "CLOB", list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, String sqlType, TgSqlColumn actual) {
        assertEquals(name, actual.getName());
        assertEquals(type, actual.getDataType());
        assertEquals(sqlType, actual.getSqlType());
    }

    @Test
    void insertLiteral() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            { // null
                var sql = "insert into " + TEST + " values(0, null)";
                try (var ps = session.createStatement(sql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
            {
                var sql = "insert into " + TEST + " values(1, 'abc')";
                try (var ps = session.createStatement(sql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
        });

        assertSelectCast();
    }

    @Test
    void insertCast() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            { // null
                var sql = "insert into " + TEST + " values(0, cast(null as clob))";
                try (var ps = session.createStatement(sql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
            {
                var sql = "insert into " + TEST + " values(1, cast('abc' as clob))";
                try (var ps = session.createStatement(sql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
        });

        assertSelectCast();
    }

    @Test
    void insertPrepared() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addClob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addClob("value", (TgClob) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-clob-insert", ".dat");
                try {
                    IceaxeFileUtil.writeString(path, "abc", StandardOpenOption.TRUNCATE_EXISTING);
                    var clob = TgClob.of(path);
                    var parameter = TgBindParameters.of().addInt("pk", 1).addClob("value", clob);
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertTrue(Files.exists(path));
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                    assertTrue(Files.exists(path));
                } finally {
                    deleteSafe(path);
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void insertPreparedTempClob(boolean deleteOnExecuteFinished) throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addClob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addClob("value", (TgClob) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                Path path = null;
                try (var clob = IceaxeObjectFactory.getDefaultInstance().createClob("abc", deleteOnExecuteFinished)) {
                    path = clob.getPath();
                    assertTrue(Files.exists(path));

                    var parameter = TgBindParameters.of().addInt("pk", 1).addClob("value", clob);
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        if (deleteOnExecuteFinished) {
                            assertFalse(Files.exists(path));
                        } else {
                            assertTrue(Files.exists(path));
                        }
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }

                    if (deleteOnExecuteFinished) {
                        assertFalse(Files.exists(path));
                    } else {
                        assertTrue(Files.exists(path));
                    }
                } finally {
                    if (path != null) {
                        assertFalse(Files.exists(path));
                        deleteSafe(path);
                    }
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertPreparedPath() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addClob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addClob("value", (Path) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-clob-insert", ".dat");
                try {
                    IceaxeFileUtil.writeString(path, "abc", StandardOpenOption.TRUNCATE_EXISTING);
                    var parameter = TgBindParameters.of().addInt("pk", 1).addClob("value", path);
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertTrue(Files.exists(path));
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                    assertTrue(Files.exists(path));
                } finally {
                    deleteSafe(path);
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertPreparedReader() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addClob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addClob("value", (Reader) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var reader = new StringReader("abc");
                    var parameter = TgBindParameters.of().addInt("pk", 1).addClob("value", reader);
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertPreparedString() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addClob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addClob("value", (String) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var parameter = TgBindParameters.of().addInt("pk", 1).addClob("value", "abc");
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertBind() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pk = TgBindVariable.ofInt("pk");
            var value = TgBindVariable.ofClob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((TgClob) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-clob-insert", ".dat");
                try {
                    IceaxeFileUtil.writeString(path, "abc", StandardOpenOption.TRUNCATE_EXISTING);
                    var clob = TgClob.of(path);
                    var parameter = TgBindParameters.of(pk.bind(1), value.bind(clob));
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertTrue(Files.exists(path));
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                    assertTrue(Files.exists(path));
                } finally {
                    deleteSafe(path);
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertBindPath() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pk = TgBindVariable.ofInt("pk");
            var value = TgBindVariable.ofClob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((Path) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-clob-insert", ".dat");
                try {
                    IceaxeFileUtil.writeString(path, "abc", StandardOpenOption.TRUNCATE_EXISTING);
                    var parameter = TgBindParameters.of(pk.bind(1), value.bind(path));
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertTrue(Files.exists(path));
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                    assertTrue(Files.exists(path));
                } finally {
                    deleteSafe(path);
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertBindReader() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pk = TgBindVariable.ofInt("pk");
            var value = TgBindVariable.ofClob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((Reader) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var reader = new StringReader("abc");
                    var parameter = TgBindParameters.of(pk.bind(1), value.bind(reader));
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertBindString() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pk = TgBindVariable.ofInt("pk");
            var value = TgBindVariable.ofClob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((String) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var parameter = TgBindParameters.of(pk.bind(1), value.bind("abc"));
                    try {
                        transaction.executeAndGetCount(ps, parameter);
                    } catch (TsurugiTransactionException e) {
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertUserEntity() throws Exception {
        class TestEntity {
            final int pk;
            final TgClob value;

            TestEntity(int pk, TgClob value) {
                this.pk = pk;
                this.value = value;
            }

            public int getPk() {
                return pk;
            }

            public TgClob getValue() {
                return value;
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addClob("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var entity = new TestEntity(0, null);
                    transaction.executeAndGetCount(ps, entity);
                }
                var path = Files.createTempFile("iceaxe-dbtest-clob-insert", ".dat");
                try {
                    IceaxeFileUtil.writeString(path, "abc", StandardOpenOption.TRUNCATE_EXISTING);
                    var clob = TgClob.of(path);
                    var entity = new TestEntity(1, clob);
                    try {
                        transaction.executeAndGetCount(ps, entity);
                    } catch (TsurugiTransactionException e) {
                        assertTrue(Files.exists(path));
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                    assertTrue(Files.exists(path));
                } finally {
                    deleteSafe(path);
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertUserEntityPath() throws Exception {
        class TestEntity {
            final int pk;
            final Path path;

            TestEntity(int pk, Path path) {
                this.pk = pk;
                this.path = path;
            }

            public int getPk() {
                return pk;
            }

            public Path getPath() {
                return path;
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addClobPath("value", TestEntity::getPath);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var entity = new TestEntity(0, null);
                    transaction.executeAndGetCount(ps, entity);
                }
                var path = Files.createTempFile("iceaxe-dbtest-clob-insert", ".dat");
                try {
                    IceaxeFileUtil.writeString(path, "abc", StandardOpenOption.TRUNCATE_EXISTING);
                    var entity = new TestEntity(1, path);
                    try {
                        transaction.executeAndGetCount(ps, entity);
                    } catch (TsurugiTransactionException e) {
                        assertTrue(Files.exists(path));
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                    assertTrue(Files.exists(path));
                } finally {
                    deleteSafe(path);
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    @Test
    void insertUserEntityString() throws Exception {
        class TestEntity {
            final int pk;
            final String value;

            TestEntity(int pk, String value) {
                this.pk = pk;
                this.value = value;
            }

            public int getPk() {
                return pk;
            }

            public String getValue() {
                return value;
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addClobString("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var entity = new TestEntity(0, null);
                    transaction.executeAndGetCount(ps, entity);
                }
                {
                    var entity = new TestEntity(1, "abc");
                    try {
                        transaction.executeAndGetCount(ps, entity);
                    } catch (TsurugiTransactionException e) {
                        assertPrivilegedMode(e);
                        transaction.rollback();
                        return false;
                    }
                }
            }
            return true;
        });

        if (inserted) {
            assertSelect();
        }
    }

    private static void assertPrivilegedMode(TsurugiTransactionException e) throws TsurugiTransactionException {
        if (e.getDiagnosticCode() == CoreServiceCode.OPERATION_DENIED) {
            return;
        }
        throw e;
    }

    private void assertSelect() throws Exception {
        assertSelectResultRecord();
        assertSelectResultEntity();
        assertSelectUserEntity();
        assertSelectSingle();
        assertSelectCast();
    }

    private void assertSelectResultRecord() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var sql = "select * from " + TEST + " order by pk";
            var resultMapping = TgResultMapping.of(record -> record);
            try (var ps = session.createQuery(sql, resultMapping)) {
                int[] i = { 0 };
                transaction.executeAndForEach(ps, record -> {
                    int pk = record.getInt("pk");
                    TgClobReference value = record.getClobOrNull("value");
                    switch (i[0]++) {
                    case 0:
                        assertEquals(0, pk);
                        assertNull(value);
                        break;
                    case 1:
                        assertEquals(1, pk);
                        assertEquals("abc", value.readString());
                        break;
                    }
                });
                assertEquals(2, i[0]);
            }
        });
    }

    private void assertSelectResultEntity() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select * from " + TEST + " order by pk";
        var list = tm.executeAndGetList(sql);
        assertEquals(2, list.size());
        {
            var entity = list.get(0);
            assertEquals(0, entity.getInt("pk"));
            assertNull(entity.getClobOrNull("value"));
        }
        {
            var entity = list.get(1);
            assertEquals(1, entity.getInt("pk"));
            try (TgClob value = entity.getClobOrNull("value")) {
                assertEquals("abc", value.readString());
            }
        }
    }

    private void assertSelectUserEntity() throws Exception {
        class TestEntity {
            int pk;
            TgClob value;

            public void setPk(int pk) {
                this.pk = pk;
            }

            public void setValue(TgClob value) {
                this.value = value;
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select * from " + TEST + " order by pk";
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt(TestEntity::setPk) //
                .addClob(TestEntity::setValue);
        var list = tm.executeAndGetList(sql, resultMapping);
        assertEquals(2, list.size());
        {
            var entity = list.get(0);
            assertEquals(0, entity.pk);
            assertNull(entity.value);
        }
        {
            var entity = list.get(1);
            assertEquals(1, entity.pk);
            Path path;
            try (TgClob value = entity.value) {
                path = value.getPath();
                assertEquals("abc", value.readString());
            }
            assertFalse(Files.exists(path));
        }
    }

    private void assertSelectSingle() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select value from " + TEST + " order by pk";
        var resultMapping = TgSingleResultMapping.ofClob();
        List<TgClob> list = tm.executeAndGetList(sql, resultMapping);
        assertEquals(2, list.size());
        {
            var value = list.get(0);
            assertNull(value);
        }
        {
            Path path;
            try (TgClob value = list.get(1)) {
                path = value.getPath();
                assertEquals("abc", value.readString());
            }
            assertFalse(Files.exists(path));
        }
    }

    private void assertSelectCast() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select pk, cast(value as varchar) from " + TEST + " order by pk";
        List<TsurugiResultEntity> list = tm.executeAndGetList(sql);
        assertEquals(2, list.size());
        {
            var entity = list.get(0);
            assertEquals(0, entity.getInt("pk"));
            assertNull(entity.getStringOrNull(1));
        }
        {
            var entity = list.get(1);
            assertEquals(1, entity.getInt("pk"));
            assertEquals("abc", entity.getStringOrNull(1));
        }
    }

    @Test
    void select() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var sql = "select * from " + TEST + " order by pk";
            try (var ps = session.createQuery(sql)) {
                var list = transaction.executeAndGetList(ps);
                assertEquals(0, list.size());
            }
        });
    }

    private void deleteSafe(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOG.debug("delete file error", e);
        }
    }
}
