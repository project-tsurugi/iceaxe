package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.sql.result.mapping.TgSingleResultMapping;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgBlobReference;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.exception.CoreServiceCode;

/**
 * BLOB test
 */
class DbBlobTest extends DbTestTableTester {

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
                + "  value blob" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getLowColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, list.get(0));
        assertColumn("value", TgDataType.BLOB, list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type.getLowDataType(), actual.getAtomType());
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
                var sql = "insert into " + TEST + " values(1, X'123456')";
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
                var sql = "insert into " + TEST + " values(0, cast(null as blob))";
                try (var ps = session.createStatement(sql)) {
                    transaction.executeAndGetCount(ps);
                }
            }
            {
                var sql = "insert into " + TEST + " values(1, cast(X'123456' as blob))";
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
            var variables = TgBindVariables.of().addInt("pk").addBlob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addBlob("value", (TgBlob) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-blob-insert", ".dat");
                try {
                    Files.write(path, new byte[] { 0x12, 0x34, 0x56 }, StandardOpenOption.TRUNCATE_EXISTING);
                    var blob = TgBlob.of(path);
                    var parameter = TgBindParameters.of().addInt("pk", 1).addBlob("value", blob);
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
    void insertPreparedTempBlob(boolean deleteOnExecuteFinished) throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addBlob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addBlob("value", (TgBlob) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                Path path = null;
                try (var blob = IceaxeObjectFactory.getDefaultInstance().createBlob(new byte[] { 0x12, 0x34, 0x56 }, deleteOnExecuteFinished)) {
                    path = blob.getPath();
                    assertTrue(Files.exists(path));

                    var parameter = TgBindParameters.of().addInt("pk", 1).addBlob("value", blob);
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
            var variables = TgBindVariables.of().addInt("pk").addBlob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addBlob("value", (Path) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-blob-insert", ".dat");
                try {
                    Files.write(path, new byte[] { 0x12, 0x34, 0x56 }, StandardOpenOption.TRUNCATE_EXISTING);
                    var parameter = TgBindParameters.of().addInt("pk", 1).addBlob("value", path);
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
    void insertPreparedInputStream() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addBlob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addBlob("value", (InputStream) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var is = new ByteArrayInputStream(new byte[] { 0x12, 0x34, 0x56 });
                    var parameter = TgBindParameters.of().addInt("pk", 1).addBlob("value", is);
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
    void insertPreparedBytes() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of().addInt("pk").addBlob("value");
            var parameterMapping = TgParameterMapping.of(variables);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of().addInt("pk", 0).addBlob("value", (byte[]) null);
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var parameter = TgBindParameters.of().addInt("pk", 1).addBlob("value", new byte[] { 0x12, 0x34, 0x56 });
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
            var value = TgBindVariable.ofBlob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((TgBlob) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-blob-insert", ".dat");
                try {
                    Files.write(path, new byte[] { 0x12, 0x34, 0x56 }, StandardOpenOption.TRUNCATE_EXISTING);
                    var blob = TgBlob.of(path);
                    var parameter = TgBindParameters.of(pk.bind(1), value.bind(blob));
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
            var value = TgBindVariable.ofBlob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((Path) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                var path = Files.createTempFile("iceaxe-dbtest-blob-insert", ".dat");
                try {
                    Files.write(path, new byte[] { 0x12, 0x34, 0x56 }, StandardOpenOption.TRUNCATE_EXISTING);
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
    void insertBindInputStream() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pk = TgBindVariable.ofInt("pk");
            var value = TgBindVariable.ofBlob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((InputStream) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var is = new ByteArrayInputStream(new byte[] { 0x12, 0x34, 0x56 });
                    var parameter = TgBindParameters.of(pk.bind(1), value.bind(is));
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
    void insertBindBytes() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pk = TgBindVariable.ofInt("pk");
            var value = TgBindVariable.ofBlob("value");
            var parameterMapping = TgParameterMapping.of(pk, value);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var parameter = TgBindParameters.of(pk.bind(0), value.bind((byte[]) null));
                    transaction.executeAndGetCount(ps, parameter);
                }
                {
                    var parameter = TgBindParameters.of(pk.bind(1), value.bind(new byte[] { 0x12, 0x34, 0x56 }));
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
            final TgBlob value;

            TestEntity(int pk, TgBlob value) {
                this.pk = pk;
                this.value = value;
            }

            public int getPk() {
                return pk;
            }

            public TgBlob getValue() {
                return value;
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addBlob("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var entity = new TestEntity(0, null);
                    transaction.executeAndGetCount(ps, entity);
                }
                var path = Files.createTempFile("iceaxe-dbtest-blob-insert", ".dat");
                try {
                    Files.write(path, new byte[] { 0x12, 0x34, 0x56 }, StandardOpenOption.TRUNCATE_EXISTING);
                    var blob = TgBlob.of(path);
                    var entity = new TestEntity(1, blob);
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
                    .addBlobPath("value", TestEntity::getPath);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var entity = new TestEntity(0, null);
                    transaction.executeAndGetCount(ps, entity);
                }
                var path = Files.createTempFile("iceaxe-dbtest-blob-insert", ".dat");
                try {
                    Files.write(path, new byte[] { 0x12, 0x34, 0x56 }, StandardOpenOption.TRUNCATE_EXISTING);
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
    void insertUserEntityBytes() throws Exception {
        class TestEntity {
            final int pk;
            final byte[] value;

            TestEntity(int pk, byte[] value) {
                this.pk = pk;
                this.value = value;
            }

            public int getPk() {
                return pk;
            }

            public byte[] getValue() {
                return value;
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        boolean inserted = tm.execute(transaction -> {
            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addBlobBytes("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                { // null
                    var entity = new TestEntity(0, null);
                    transaction.executeAndGetCount(ps, entity);
                }
                {
                    var entity = new TestEntity(1, new byte[] { 0x12, 0x34, 0x56 });
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
                    TgBlobReference value = record.getBlobOrNull("value");
                    switch (i[0]++) {
                    case 0:
                        assertEquals(0, pk);
                        assertNull(value);
                        break;
                    case 1:
                        assertEquals(1, pk);
                        assertArrayEquals(new byte[] { 0x12, 0x34, 0x56 }, value.readAllBytes());
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
            assertNull(entity.getBlobOrNull("value"));
        }
        {
            var entity = list.get(1);
            assertEquals(1, entity.getInt("pk"));
            try (TgBlob value = entity.getBlobOrNull("value")) {
                assertArrayEquals(new byte[] { 0x12, 0x34, 0x56 }, value.readAllBytes());
            }
        }
    }

    private void assertSelectUserEntity() throws Exception {
        class TestEntity {
            int pk;
            TgBlob value;

            public void setPk(int pk) {
                this.pk = pk;
            }

            public void setValue(TgBlob value) {
                this.value = value;
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select * from " + TEST + " order by pk";
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt(TestEntity::setPk) //
                .addBlob(TestEntity::setValue);
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
            try (TgBlob value = entity.value) {
                path = value.getPath();
                assertArrayEquals(new byte[] { 0x12, 0x34, 0x56 }, value.readAllBytes());
            }
            assertFalse(Files.exists(path));
        }
    }

    private void assertSelectSingle() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select value from " + TEST + " order by pk";
        var resultMapping = TgSingleResultMapping.ofBlob();
        List<TgBlob> list = tm.executeAndGetList(sql, resultMapping);
        assertEquals(2, list.size());
        {
            var value = list.get(0);
            assertNull(value);
        }
        {
            Path path;
            try (TgBlob value = list.get(1)) {
                path = value.getPath();
                assertArrayEquals(new byte[] { 0x12, 0x34, 0x56 }, value.readAllBytes());
            }
            assertFalse(Files.exists(path));
        }
    }

    private void assertSelectCast() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var sql = "select pk, cast(value as varbinary) from " + TEST + " order by pk";
        List<TsurugiResultEntity> list = tm.executeAndGetList(sql);
        assertEquals(2, list.size());
        {
            var entity = list.get(0);
            assertEquals(0, entity.getInt("pk"));
            assertNull(entity.getBytesOrNull(1));
        }
        {
            var entity = list.get(1);
            assertEquals(1, entity.getInt("pk"));
            assertArrayEquals(new byte[] { 0x12, 0x34, 0x56 }, entity.getBytesOrNull(1));
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
