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

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.type.IceaxeObjectFactory;
import com.tsurugidb.iceaxe.sql.type.TgBlobReference;
import com.tsurugidb.iceaxe.sql.type.TgClobReference;
import com.tsurugidb.iceaxe.sql.type.TgLobPersistenceType;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

public class DbIceaxeObjectFactoryTest extends DbTestTableTester {

    @Test
    void setDefaultPersistenceType() {
        var factory = new IceaxeObjectFactory();
        assertEquals(TgLobPersistenceType.FILE, factory.getDefaultPersistenceType());

        factory.setDefaultPersistenceType(TgLobPersistenceType.MEMORY);
        assertEquals(TgLobPersistenceType.MEMORY, factory.getDefaultPersistenceType());
    }

    @Test
    @SuppressWarnings("removal")
    void createBlob_Path() {
        var factory = new IceaxeObjectFactory();

        var path = Path.of("test_blob.dat");
        var blob = factory.createBlob(path);

        assertEquals(path, blob.getPath());
    }

    @Test
    void createBlob_Path_persistenceType() throws Exception {
        var factory = new IceaxeObjectFactory();

        {
            var path = Path.of("test_blob.dat");
            var blob = factory.createBlob(path, TgLobPersistenceType.FILE);

            assertEquals(path, blob.getPath());
        }
        {
            var path = factory.createTempFilePath();
            Files.write(path, new byte[] { 1, 2, 3 });
            var blob = factory.createBlob(path, TgLobPersistenceType.MEMORY);

            assertNull(blob.getPath());
            assertArrayEquals(new byte[] { 1, 2, 3 }, blob.readAllBytes());
        }
    }

    @Test
    @SuppressWarnings("removal")
    void createBlob_InputStream_deleteOnExecuteFinished() throws Exception {
        var factory = new IceaxeObjectFactory();

        var data = new byte[] { 1, 2, 3 };
        for (boolean deleteOnExecuteFinished : new boolean[] { true, false }) {
            try (var blob = factory.createBlob(new ByteArrayInputStream(data), deleteOnExecuteFinished)) {
                assertNotNull(blob.getPath());
                assertEquals(deleteOnExecuteFinished, blob.isDeleteOnExecuteFinished());
                assertArrayEquals(data, blob.readAllBytes());
            }
        }
    }

    @Test
    void createBlob_InputStream_persistenceType() throws Exception {
        var factory = new IceaxeObjectFactory();

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
    @SuppressWarnings("removal")
    void createBlob_bytes_deleteOnExecuteFinished() throws Exception {
        var factory = new IceaxeObjectFactory();

        var data = new byte[] { 1, 2, 3 };
        for (boolean deleteOnExecuteFinished : new boolean[] { true, false }) {
            try (var blob = factory.createBlob(data, deleteOnExecuteFinished)) {
                assertNotNull(blob.getPath());
                assertEquals(deleteOnExecuteFinished, blob.isDeleteOnExecuteFinished());
                assertArrayEquals(data, blob.readAllBytes());
            }
        }
    }

    @Test
    void createBlob_bytes_persistenceType() throws Exception {
        var factory = new IceaxeObjectFactory();

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
        var factory = new IceaxeObjectFactory();

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
    @SuppressWarnings("removal")
    void createClob_Path() {
        var factory = new IceaxeObjectFactory();

        var path = Path.of("test_clob.dat");
        var clob = factory.createClob(path);

        assertEquals(path, clob.getPath());
    }

    @Test
    void createClob_Path_persistenceType() throws Exception {
        var factory = new IceaxeObjectFactory();

        {
            var path = Path.of("test_clob.dat");
            var clob = factory.createClob(path, TgLobPersistenceType.FILE);

            assertEquals(path, clob.getPath());
        }
        {
            var path = factory.createTempFilePath();
            Files.write(path, "abc".getBytes());
            var clob = factory.createClob(path, TgLobPersistenceType.MEMORY);

            assertNull(clob.getPath());
            assertEquals("abc", clob.readString());
        }
    }

    @Test
    @SuppressWarnings("removal")
    void createClob_Reader_deleteOnExecuteFinished() throws Exception {
        var factory = new IceaxeObjectFactory();

        var data = "abc";
        for (boolean deleteOnExecuteFinished : new boolean[] { true, false }) {
            try (var clob = factory.createClob(new StringReader(data), deleteOnExecuteFinished)) {
                assertNotNull(clob.getPath());
                assertEquals(deleteOnExecuteFinished, clob.isDeleteOnExecuteFinished());
                assertEquals(data, clob.readString());
            }
        }
    }

    @Test
    void createClob_Reader_persistenceType() throws Exception {
        var factory = new IceaxeObjectFactory();

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
    @SuppressWarnings("removal")
    void createClob_String_deleteOnExecuteFinished() throws Exception {
        var factory = new IceaxeObjectFactory();

        var data = "abc";
        for (boolean deleteOnExecuteFinished : new boolean[] { true, false }) {
            try (var clob = factory.createClob(data, deleteOnExecuteFinished)) {
                assertNotNull(clob.getPath());
                assertEquals(deleteOnExecuteFinished, clob.isDeleteOnExecuteFinished());
                assertEquals(data, clob.readString());
            }
        }
    }

    @Test
    void createClob_String_persistenceType() throws Exception {
        var factory = new IceaxeObjectFactory();

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
        var factory = new IceaxeObjectFactory();

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
}
