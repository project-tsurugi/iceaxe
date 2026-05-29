/*
 * Copyright 2023-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.session.TgLobTransferType;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
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
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.util.IceaxeFileUtil;

/**
 * CLOB test2
 */
class DbClob2Test extends DbTestTableTester {

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

    private static List<String> getTestDataList() {
        return Arrays.asList( //
                null, //
                "", //
                "abc", //
                createTestData(1024 * 1024), //
                createTestData(1024 * 1024 - 1), //
                createTestData(1024 * 1024 + 1) //
        );
    }

    private static String createTestData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) ('A' + (i % ('Z' - 'A' + 1)));
        }
        return new String(data);
    }

    private static TsurugiSession createTestSession(String type) throws IOException, InterruptedException {
        var sessionOption = DbTestConnector.createSessionOption();

        var lobTransferType = TgLobTransferType.valueOf(type.toUpperCase());
        sessionOption.setLobTransferType(lobTransferType);

        var session = DbTestConnector.createSession(sessionOption);
        var resultType = session.getLobTransferType();
        switch (lobTransferType) {
        case DEFAULT:
            assertTrue(resultType == TgLobTransferType.RELAY || resultType == TgLobTransferType.NOT_USE);
            break;
        case NOT_USE:
            assertEquals(TgLobTransferType.NOT_USE, resultType);
            break;
        case PRIVILEGED:
            assertEquals(TgLobTransferType.PRIVILEGED, resultType);
            break;
        case RELAY:
            assertEquals(TgLobTransferType.RELAY, resultType);
            break;
        }
        return session;
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void insertLiteral(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            session.getSessionOption().setTimeout(TgTimeoutKey.DEFAULT, 60, TimeUnit.SECONDS);

            var list = getTestDataList();

            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                int pk = 0;
                for (String data : list) {
                    String value = (data != null) ? ("'" + data + "'") : "null";
                    var sql = String.format("insert into " + TEST + " values(%d, %s)", pk++, value);
                    try (var ps = session.createStatement(sql)) {
                        transaction.executeAndGetCount(ps);
                    }
                }
            });

            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                selectCast(session, list);
            } else {
                assertSelect(session, list);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindParameters_TgClob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of() //
                    .addInt("pk") //
                    .addClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(variables))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    var lobFactory = session.getLobFactory();

                    int pk = 0;
                    for (String data : list) {
                        try (TgClob clob = lobFactory.createClob(data)) {
                            var parameter = TgBindParameters.of() //
                                    .addInt("pk", pk++) //
                                    .addClob("value", clob);
                            ps.explain(parameter);
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindParameters_TgRemoteClob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of() //
                    .addInt("pk") //
                    .addClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(variables))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    var lobFactory = session.getLobFactory();

                    int pk = 0;
                    for (String data : list) {
                        try (TgRemoteClob clob = lobFactory.uploadClob(data)) {
                            var parameter = TgBindParameters.of() //
                                    .addInt("pk", pk++) //
                                    .addClob("value", clob);
                            ps.explain(parameter);
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindParameters_Path(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of() //
                    .addInt("pk") //
                    .addClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(variables))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        Path path = createTempFile(data);
                        try {
                            var parameter = TgBindParameters.of() //
                                    .addInt("pk", pk++) //
                                    .addClob("value", path);
                            ps.explain(parameter);
                            transaction.executeAndGetCount(ps, parameter);
                        } finally {
                            if (path != null) {
                                Files.deleteIfExists(path);
                            }
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    private static Path createTempFile(String data) throws IOException {
        if (data == null) {
            return null;
        }

        Path dir = IceaxeObjectFactory.getDefaultInstance().getTempDirectory();
        Path path = Files.createTempFile(dir, "iceaxe-dbtest-clob2-", ".dat");

        Files.write(path, data.getBytes(StandardCharsets.UTF_8));
        return path;
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindParameters_InputStream(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of() //
                    .addInt("pk") //
                    .addClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(variables))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        try (Reader reader = (data != null) ? new StringReader(data) : null) {
                            var parameter = TgBindParameters.of() //
                                    .addInt("pk", pk++) //
                                    .addClob("value", reader);
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindParameters_bytes(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var variables = TgBindVariables.of() //
                    .addInt("pk") //
                    .addClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(variables))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        var parameter = TgBindParameters.of() //
                                .addInt("pk", pk++) //
                                .addClob("value", data);
                        transaction.executeAndGetCount(ps, parameter);
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindVariable_TgClob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pkVariable = TgBindVariable.ofInt("pk");
            var valueVariable = TgBindVariable.ofClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(pkVariable, valueVariable))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    var lobFactory = session.getLobFactory();

                    int pk = 0;
                    for (String data : list) {
                        try (TgClob clob = lobFactory.createClob(data)) {
                            var parameter = TgBindParameters.of(pkVariable.bind(pk++), valueVariable.bind(clob));
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindVariable_TgRemoteClob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pkVariable = TgBindVariable.ofInt("pk");
            var valueVariable = TgBindVariable.ofClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(pkVariable, valueVariable))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    var lobFactory = session.getLobFactory();

                    int pk = 0;
                    for (String data : list) {
                        try (TgRemoteClob clob = lobFactory.uploadClob(data)) {
                            var parameter = TgBindParameters.of(pkVariable.bind(pk++), valueVariable.bind(clob));
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindVariable_Path(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pkVariable = TgBindVariable.ofInt("pk");
            var valueVariable = TgBindVariable.ofClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(pkVariable, valueVariable))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        Path path = createTempFile(data);
                        try {
                            var parameter = TgBindParameters.of(pkVariable.bind(pk++), valueVariable.bind(path));
                            transaction.executeAndGetCount(ps, parameter);
                        } finally {
                            if (path != null) {
                                Files.deleteIfExists(path);
                            }
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindVariable_InputStream(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pkVariable = TgBindVariable.ofInt("pk");
            var valueVariable = TgBindVariable.ofClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(pkVariable, valueVariable))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        try (Reader reader = (data != null) ? new StringReader(data) : null) {
                            var parameter = TgBindParameters.of(pkVariable.bind(pk++), valueVariable.bind(reader));
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void bindVariable_bytes(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var pkVariable = TgBindVariable.ofInt("pk");
            var valueVariable = TgBindVariable.ofClob("value");
            try (var ps = session.createStatement(sql, TgParameterMapping.of(pkVariable, valueVariable))) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        var parameter = TgBindParameters.of(pkVariable.bind(pk++), valueVariable.bind(data));
                        transaction.executeAndGetCount(ps, parameter);
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void userEntity_TgClob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            class TestEntity {
                int pk;
                TgClob value;

                public TestEntity(int pk, TgClob value) {
                    this.pk = pk;
                    this.value = value;
                }

                public int getPk() {
                    return this.pk;
                }

                public TgClob getValue() {
                    return this.value;
                }
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addClob("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    var lobFactory = session.getLobFactory();

                    int pk = 0;
                    for (String data : list) {
                        try (TgClob clob = lobFactory.createClob(data)) {
                            var parameter = new TestEntity(pk++, clob);
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void userEntity_TgRemoteClob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            class TestEntity {
                int pk;
                TgRemoteClob value;

                public TestEntity(int pk, TgRemoteClob value) {
                    this.pk = pk;
                    this.value = value;
                }

                public int getPk() {
                    return this.pk;
                }

                public TgRemoteClob getValue() {
                    return this.value;
                }
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addRemoteClob("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    var lobFactory = session.getLobFactory();

                    int pk = 0;
                    for (String data : list) {
                        try (TgRemoteClob clob = lobFactory.uploadClob(data)) {
                            var parameter = new TestEntity(pk++, clob);
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void userEntity_Path(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            class TestEntity {
                int pk;
                Path value;

                public TestEntity(int pk, Path value) {
                    this.pk = pk;
                    this.value = value;
                }

                public int getPk() {
                    return this.pk;
                }

                public Path getValue() {
                    return this.value;
                }
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addClobPath("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        Path path = createTempFile(data);
                        try {
                            var parameter = new TestEntity(pk++, path);
                            transaction.executeAndGetCount(ps, parameter);
                        } finally {
                            if (path != null) {
                                Files.deleteIfExists(path);
                            }
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void userEntity_bytes(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            class TestEntity {
                int pk;
                String value;

                public TestEntity(int pk, String value) {
                    this.pk = pk;
                    this.value = value;
                }

                public int getPk() {
                    return this.pk;
                }

                public String getValue() {
                    return this.value;
                }
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .addClobString("value", TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        var parameter = new TestEntity(pk++, data);
                        transaction.executeAndGetCount(ps, parameter);
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void userEntity_Object(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            class TestEntity {
                int pk;
                String value;

                public TestEntity(int pk, String value) {
                    this.pk = pk;
                    this.value = value;
                }

                public int getPk() {
                    return this.pk;
                }

                public String getValue() {
                    return this.value;
                }
            }

            var list = getTestDataList();

            var sql = "insert into " + TEST + " values(:pk, :value)";
            var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                    .addInt("pk", TestEntity::getPk) //
                    .add("value", TgDataType.CLOB, TestEntity::getValue);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    int pk = 0;
                    for (String data : list) {
                        var parameter = new TestEntity(pk++, data);
                        transaction.executeAndGetCount(ps, parameter);
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEFAULT", "NOT_USE", "PRIVILEGED", "RELAY" })
    void singleParameterMapping(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = createTestSession(lobTransferType)) {
            if (session.getLobTransferType() == TgLobTransferType.NOT_USE) {
                return;
            }

            dropTestTable();
            String ddl = "create table " + TEST + "(" //
                    + "  pk int primary key generated always as identity (start 0 minvalue 0)," //
                    + "  value clob" //
                    + ")";
            executeDdl(session, ddl);

            var list = getTestDataList();

            var sql = "insert into " + TEST + "(value) values(:value)";
            var parameterMapping = TgParameterMapping.ofSingle("value", TgDataType.CLOB);
            try (var ps = session.createStatement(sql, parameterMapping)) {
                var tm = createTransactionManagerOcc(session);
                tm.execute(transaction -> {
                    var lobFactory = session.getLobFactory();

                    for (String data : list) {
                        try (TgClob clob = lobFactory.createClob(data)) {
                            var parameter = clob;
                            transaction.executeAndGetCount(ps, parameter);
                        }
                    }
                });
            }

            assertSelect(session, list);
        }
    }

    private void assertSelect(TsurugiSession session, List<String> excepctedList) throws Exception {
        selectResultRecord(session, excepctedList);
        selectResultEntity(session, excepctedList);
        selectUserEntity(session, excepctedList);
        selectSingle(session, excepctedList);
        selectCast(session, excepctedList);
    }

    private void selectResultRecord(TsurugiSession session, List<String> excepctedList) throws Exception {
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var sql = "select * from " + TEST + " order by pk";
            var resultMapping = TgResultMapping.of(record -> record);
            try (var ps = session.createQuery(sql, resultMapping)) {
                int[] i = { 0 };
                transaction.executeAndForEach(ps, record -> {
                    int expectedPk = i[0];
                    var expectedValue = excepctedList.get(i[0]++);

                    int pk = record.getInt("pk");
                    TgClobReference value = record.getClobOrNull("value");

                    assertEquals(expectedPk, pk);
                    assertClob(expectedValue, value);
                });
                assertEquals(excepctedList.size(), i[0]);
            }
        });
    }

    private void selectResultEntity(TsurugiSession session, List<String> excepctedList) throws Exception {
        var tm = createTransactionManagerOcc(session);
        var sql = "select * from " + TEST + " order by pk";
        var list = tm.executeAndGetList(sql);
        assertEquals(excepctedList.size(), list.size());

        int i = 0;
        for (var entity : list) {
            int expectedPk = i;
            var expectedValue = excepctedList.get(i++);

            int pk = entity.getInt("pk");
            TgClob value = entity.getClobOrNull("value");

            assertEquals(expectedPk, pk);
            assertClob(expectedValue, value);
        }
    }

    private void selectUserEntity(TsurugiSession session, List<String> excepctedList) throws Exception {
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

        var tm = createTransactionManagerOcc(session);
        var sql = "select * from " + TEST + " order by pk";
        var resultMapping = TgResultMapping.of(TestEntity::new) //
                .addInt(TestEntity::setPk) //
                .addClob(TestEntity::setValue);
        var list = tm.executeAndGetList(sql, resultMapping);
        assertEquals(excepctedList.size(), list.size());

        int i = 0;
        for (var entity : list) {
            int expectedPk = i;
            var expectedValue = excepctedList.get(i++);

            int pk = entity.pk;
            TgClob value = entity.value;

            assertEquals(expectedPk, pk);
            assertClob(expectedValue, value);
        }
    }

    private void selectSingle(TsurugiSession session, List<String> excepctedList) throws Exception {
        var tm = createTransactionManagerOcc(session);
        var sql = "select value from " + TEST + " order by pk";
        var resultMapping = TgSingleResultMapping.ofClob();
        List<TgClob> list = tm.executeAndGetList(sql, resultMapping);
        assertEquals(excepctedList.size(), list.size());

        int i = 0;
        for (var value : list) {
            var expectedValue = excepctedList.get(i++);

            assertClob(expectedValue, value);
        }
    }

    private void selectCast(TsurugiSession session, List<String> excepctedList) throws Exception {
        var tm = createTransactionManagerOcc(session);
        var sql = "select pk, cast(value as varchar) from " + TEST;
        var map = where(excepctedList);
        if (map != null) {
            sql += map.keySet().stream().map(n -> n.toString()).collect(Collectors.joining(",", " where pk in (", ")"));
        } else {
            map = new HashMap<>(excepctedList.size());
            int pk = 0;
            for (var value : excepctedList) {
                map.put(pk++, value);
            }
        }
        sql += " order by pk";

        List<TsurugiResultEntity> list = tm.executeAndGetList(sql);
        assertEquals(map.size(), list.size());

        for (var entity : list) {
            int pk = entity.getInt("pk");
            String value = entity.getStringOrNull(1);

            String expectedValue = map.get(pk);
            assertClob(expectedValue, value);
        }
    }

    private static Map<Integer, String> where(List<String> list) {
        if (DbTestConnector.isTcp()) {
            return null;
        }

        var map = new HashMap<Integer, String>(list.size());
        int pk = 0;
        for (var value : list) {
            if (value != null && value.getBytes(StandardCharsets.UTF_8).length > 1024) {
                pk++;
                continue;
            }
            map.put(pk++, value);
        }
        return map;
    }

    private void assertClob(String expected, TgClobReference actual) throws IOException, InterruptedException, TsurugiTransactionException {
        if (expected == null) {
            assertNull(actual);
            return;
        }

        {
            var path = Files.createTempFile("iceaxe-dbtest-clob2-", ".dat");
            try {
                Files.deleteIfExists(path);
                actual.copyTo(path);

                var value = Files.readString(path);
                assertEquals(expected, value);
            } finally {
                Files.deleteIfExists(path);
            }
        }
        { // use cache
            var path = Files.createTempFile("iceaxe-dbtest-clob2-", ".dat");
            try {
                Files.deleteIfExists(path);
                actual.copyTo(path, true);

                var value = Files.readString(path);
                assertEquals(expected, value);
            } finally {
                Files.deleteIfExists(path);
            }
        }
        {
            var value = actual.readString();
            assertEquals(expected, value);
        }
    }

    private void assertClob(String expected, TgClob actual) throws IOException, InterruptedException, TsurugiTransactionException {
        if (expected == null) {
            assertNull(actual);
            return;
        }

        try (actual) {
            try (var reader = actual.openReader()) {
                var value = IceaxeFileUtil.readString(reader);
                assertEquals(expected, value);
            }
            {
                var path = Files.createTempFile("iceaxe-dbtest-clob2-", ".dat");
                try {
                    Files.deleteIfExists(path);
                    actual.copyTo(path);

                    var value = Files.readString(path);
                    assertEquals(expected, value);
                } finally {
                    Files.deleteIfExists(path);
                }
            }
            {
                var value = actual.readString();
                assertEquals(expected, value);
            }
        }
    }

    private void assertClob(String expected, String actual) throws IOException, InterruptedException, TsurugiTransactionException {
        assertEquals(expected, actual);
    }
}
