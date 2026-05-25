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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.session.TgLobTransferType;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.sql.type.TgBlobReference;
import com.tsurugidb.iceaxe.sql.type.TgClob;
import com.tsurugidb.iceaxe.sql.type.TgClobReference;
import com.tsurugidb.iceaxe.sql.type.TgLobPersistenceType;
import com.tsurugidb.iceaxe.sql.type.TgRemoteBlob;
import com.tsurugidb.iceaxe.sql.type.TgRemoteClob;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

public class DbTsurugiLobFactoryTest extends DbTestTableTester {

    @Test
    void setDefaultPersistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();
        assertNull(factory.getDefaultPersistenceType());

        factory.setDefaultPersistenceType(TgLobPersistenceType.MEMORY);
        assertEquals(TgLobPersistenceType.MEMORY, factory.getDefaultPersistenceType());
    }

    @Test
    void createBlob_Path_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        {
            var path = Path.of("test_blob.dat");
            var blob = factory.createBlob(path, TgLobPersistenceType.FILE);

            assertEquals(path, blob.getPath());
        }
        {
            var path = IceaxeObjectFactory.getDefaultInstance().createTempFilePath();
            Files.write(path, new byte[] { 1, 2, 3 });
            var blob = factory.createBlob(path, TgLobPersistenceType.MEMORY);

            assertNull(blob.getPath());
            assertArrayEquals(new byte[] { 1, 2, 3 }, blob.readAllBytes());
        }
    }

    @Test
    void createBlob_InputStream_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        var data = new byte[] { 1, 2, 3 };
        try (var blob = factory.createBlob(new ByteArrayInputStream(data), TgLobPersistenceType.FILE)) {
            assertNotNull(blob.getPath());
            assertArrayEquals(data, blob.readAllBytes());
        }
        try (var blob = factory.createBlob(new ByteArrayInputStream(data), TgLobPersistenceType.MEMORY)) {
            assertNull(blob.getPath());
            assertArrayEquals(data, blob.readAllBytes());
        }
    }

    @Test
    void createBlob_bytes_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        var data = new byte[] { 1, 2, 3 };
        try (var blob = factory.createBlob(data, TgLobPersistenceType.FILE)) {
            assertNotNull(blob.getPath());
            assertArrayEquals(data, blob.readAllBytes());
        }
        try (var blob = factory.createBlob(data, TgLobPersistenceType.MEMORY)) {
            assertNull(blob.getPath());
            assertArrayEquals(data, blob.readAllBytes());
        }
    }

    @Test
    void createBlob_BlobReference_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        testBlobReference(blobReference -> {
            try (var blob = factory.createBlob(blobReference, TgLobPersistenceType.FILE)) {
                assertNotNull(blob.getPath());
                assertArrayEquals(new byte[] { 0x12, 0x34 }, blob.readAllBytes());
            }
            try (var blob = factory.createBlob(blobReference, TgLobPersistenceType.MEMORY)) {
                assertNull(blob.getPath());
                assertArrayEquals(new byte[] { 0x12, 0x34 }, blob.readAllBytes());
            }
        });
    }

    @FunctionalInterface
    private interface BlobReferenceConsumer {
        void accept(TgBlobReference blobReference) throws IOException, InterruptedException, TsurugiTransactionException;
    }

    private void testBlobReference(BlobReferenceConsumer test) throws IOException, InterruptedException {
        dropTable(TEST);
        String sql = "create table " + TEST + "(" //
                + "  value blob" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);

        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            try (var ps = session.createStatement("insert into " + TEST + " values(X'1234')")) {
                transaction.executeStatement(ps);
            }
            try (var ps = session.createQuery("select value from " + TEST, TgResultMapping.of(record -> record))) {
                transaction.executeAndForEach(ps, record -> {
                    TgBlobReference blob = record.getBlob(0);
                    test.accept(blob);
                });
            }
        });
    }

    @Test
    void createClob_Path_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        {
            var path = Path.of("test_clob.dat");
            var clob = factory.createClob(path, TgLobPersistenceType.FILE);

            assertEquals(path, clob.getPath());
        }
        {
            var path = IceaxeObjectFactory.getDefaultInstance().createTempFilePath();
            Files.write(path, "abc".getBytes());
            var clob = factory.createClob(path, TgLobPersistenceType.MEMORY);

            assertNull(clob.getPath());
            assertEquals("abc", clob.readString());
        }
    }

    @Test
    void createClob_Reader_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        var data = "abc";
        try (var clob = factory.createClob(new StringReader(data), TgLobPersistenceType.FILE)) {
            assertNotNull(clob.getPath());
            assertEquals(data, clob.readString());
        }
        try (var clob = factory.createClob(new StringReader(data), TgLobPersistenceType.MEMORY)) {
            assertNull(clob.getPath());
            assertEquals(data, clob.readString());
        }
    }

    @Test
    void createClob_String_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        var data = "abc";
        try (var clob = factory.createClob(data, TgLobPersistenceType.FILE)) {
            assertNotNull(clob.getPath());
            assertEquals(data, clob.readString());
        }
        try (var clob = factory.createClob(data, TgLobPersistenceType.MEMORY)) {
            assertNull(clob.getPath());
            assertEquals(data, clob.readString());
        }
    }

    @Test
    void createClob_ClobReference_persistenceType() throws Exception {
        var session = getSession();
        var factory = session.getLobFactory();

        testClobReference(clobReference -> {
            try (var clob = factory.createClob(clobReference, TgLobPersistenceType.FILE)) {
                assertNotNull(clob.getPath());
                assertEquals("abc", clob.readString());
            }
            try (var clob = factory.createClob(clobReference, TgLobPersistenceType.MEMORY)) {
                assertNull(clob.getPath());
                assertEquals("abc", clob.readString());
            }
        });
    }

    @FunctionalInterface
    private interface ClobReferenceConsumer {
        void accept(TgClobReference clobReference) throws IOException, InterruptedException, TsurugiTransactionException;
    }

    private void testClobReference(ClobReferenceConsumer test) throws IOException, InterruptedException {
        dropTable(TEST);
        String sql = "create table " + TEST + "(" //
                + "  value clob" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);

        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            try (var ps = session.createStatement("insert into " + TEST + " values('abc')")) {
                transaction.executeStatement(ps);
            }
            try (var ps = session.createQuery("select value from " + TEST, TgResultMapping.of(record -> record))) {
                transaction.executeAndForEach(ps, record -> {
                    TgClobReference clob = record.getClob(0);
                    test.accept(clob);
                });
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void uploadBlob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = getLobTransferAvailableSession(lobTransferType)) {
            uploadBlob_Path(session);
            uploadBlob_InputStream(session);
            uploadBlob_bytes(session);
            uploadBlob_Blob(session);
        }
    }

    private TsurugiSession getLobTransferAvailableSession(String lobTransferType) {
        var type = TgLobTransferType.valueOf(lobTransferType);
        var sessionOption = DbTestConnector.createSessionOption();
        sessionOption.setLobTransferType(type);
        try {
            return DbTestConnector.createSession(sessionOption);
        } catch (IOException e) {
            LOG.warn(lobTransferType + " lob transfer is not available", e);
        }

        Assumptions.abort("Lob transfer is not available in the test environment");
        return null; // Unreachable, but required for compilation
    }

    private void uploadBlob_Path(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        var path = IceaxeObjectFactory.getDefaultInstance().createTempFilePath();
        Files.write(path, new byte[] { 0x12, 0x34 });
        try (TgRemoteBlob blob = factory.uploadBlob(path)) {
            assertNotNull(blob.getLowLargeObjectInfo());
        }
    }

    private void uploadBlob_InputStream(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        try (TgRemoteBlob blob = factory.uploadBlob(new ByteArrayInputStream(new byte[] { 0x12, 0x34 }))) {
            assertNotNull(blob.getLowLargeObjectInfo());
        }
    }

    private void uploadBlob_bytes(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        try (TgRemoteBlob blob = factory.uploadBlob(new byte[] { 0x12, 0x34 })) {
            assertNotNull(blob.getLowLargeObjectInfo());
        }
    }

    private void uploadBlob_Blob(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        var path = IceaxeObjectFactory.getDefaultInstance().createTempFilePath();
        Files.write(path, new byte[] { 0x12, 0x34 });
        try (TgBlob data = TgBlob.of(path); //
                TgRemoteBlob blob = factory.uploadBlob(data)) {
            assertNotNull(blob.getLowLargeObjectInfo());
        }

        try (TgBlob data = TgBlob.of(new byte[] { 0x12, 0x34 }); //
                TgRemoteBlob blob = factory.uploadBlob(data)) {
            assertNotNull(blob.getLowLargeObjectInfo());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "RELAY", "PRIVILEGED" })
    void uploadClob(String lobTransferType) throws Exception {
        assumeLobTest(lobTransferType);

        try (var session = getLobTransferAvailableSession(lobTransferType)) {
            uploadClob_Path(session);
            uploadClob_Reader(session);
            uploadClob_String(session);
            uploadClob_Clob(session);
        }
    }

    private void uploadClob_Path(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        var path = IceaxeObjectFactory.getDefaultInstance().createTempFilePath();
        Files.write(path, new byte[] { 0x12, 0x34 });
        try (TgRemoteClob clob = factory.uploadClob(path)) {
            assertNotNull(clob.getLowLargeObjectInfo());
        }
    }

    private void uploadClob_Reader(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        try (TgRemoteClob clob = factory.uploadClob(new StringReader("abc"))) {
            assertNotNull(clob.getLowLargeObjectInfo());
        }
    }

    private void uploadClob_String(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        try (TgRemoteClob clob = factory.uploadClob("abc")) {
            assertNotNull(clob.getLowLargeObjectInfo());
        }
    }

    private void uploadClob_Clob(TsurugiSession session) throws Exception {
        var factory = session.getLobFactory();

        var path = IceaxeObjectFactory.getDefaultInstance().createTempFilePath();
        Files.write(path, "abc".getBytes());
        try (TgClob data = TgClob.of(path); //
                TgRemoteClob clob = factory.uploadClob(data)) {
            assertNotNull(clob.getLowLargeObjectInfo());
        }

        try (TgClob data = TgClob.of("abc"); //
                TgRemoteClob clob = factory.uploadClob(data)) {
            assertNotNull(clob.getLowLargeObjectInfo());
        }
    }
}
