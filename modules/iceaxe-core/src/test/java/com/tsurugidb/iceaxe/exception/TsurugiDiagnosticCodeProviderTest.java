/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.iceaxe.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.tsurugidb.tsubakuro.exception.DiagnosticCode;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

class TsurugiDiagnosticCodeProviderTest {

    @Test
    void testFindDiagnosticCodeProvider() {
        assertTrue(TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(null).isEmpty());
        assertTrue(TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(new Exception()).isEmpty());
        {
            var e = new TsurugiIOException("", null);
            var actual = TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(e);
            assertSame(e, actual.get());
        }
        {
            var e = new IceaxeServerExceptionTestMock("ignore", 123);
            var actual = TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(e);
            assertEquals("TEST-00123 (MOCK_123)", actual.get().getDiagnosticCode().toString());
        }
        {
            var e = new Exception(new IceaxeServerExceptionTestMock("ignore", 123));
            var actual = TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(e);
            assertEquals("TEST-00123 (MOCK_123)", actual.get().getDiagnosticCode().toString());
        }
    }

    @Test
    void testCreateMessageServerException() {
        assertEquals("message", TsurugiDiagnosticCodeProvider.createMessage(new IceaxeServerExceptionTestMock("message", null)));
        assertEquals("MOCK_123: message", TsurugiDiagnosticCodeProvider.createMessage(new IceaxeServerExceptionTestMock("message", 123)));
    }

    @Test
    void testCreateMessageDiagnosticCode() {
        assertNull(TsurugiDiagnosticCodeProvider.createMessage((DiagnosticCode) null));
        assertEquals("SQL-01000 (SQL_SERVICE_EXCEPTION)", TsurugiDiagnosticCodeProvider.createMessage(SqlServiceCode.SQL_SERVICE_EXCEPTION));
    }
}
