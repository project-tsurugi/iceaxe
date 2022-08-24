package com.tsurugidb.iceaxe.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.nautilus_technologies.tsubakuro.exception.DiagnosticCode;
import com.nautilus_technologies.tsubakuro.exception.SqlServiceCode;

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
            var e = new IceaxeServerExceptionTestMock("ignore", "abc");
            var actual = TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(e);
            assertEquals("abc", actual.get().getLowDiagnosticCode().toString());
        }
        {
            var e = new Exception(new IceaxeServerExceptionTestMock("ignore", "abc"));
            var actual = TsurugiDiagnosticCodeProvider.findDiagnosticCodeProvider(e);
            assertEquals("abc", actual.get().getLowDiagnosticCode().toString());
        }
    }

    @Test
    void testCreateMessageServerException() {
        assertEquals("abc", TsurugiDiagnosticCodeProvider.createMessage(new IceaxeServerExceptionTestMock("ignore", "abc")));
    }

    @Test
    void testCreateMessageDiagnosticCode() {
        assertNull(TsurugiDiagnosticCodeProvider.createMessage((DiagnosticCode) null));
        assertEquals("SQL-00000 (OK)", TsurugiDiagnosticCodeProvider.createMessage(SqlServiceCode.OK));
    }
}
