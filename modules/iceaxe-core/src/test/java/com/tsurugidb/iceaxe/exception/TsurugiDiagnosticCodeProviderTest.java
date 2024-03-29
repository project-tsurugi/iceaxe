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
