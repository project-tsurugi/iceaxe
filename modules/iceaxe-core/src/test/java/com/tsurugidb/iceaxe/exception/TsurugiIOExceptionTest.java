/*
 * Copyright 2023-2025 Project Tsurugi.
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

import java.io.IOException;

import org.junit.jupiter.api.Test;

class TsurugiIOExceptionTest {

    @Test
    void constructMessageIOException() {
        var se = new IceaxeServerExceptionTestMock("test", IceaxeErrorCode.TX_LOW_ERROR);
        var target = new TsurugiIOException("test1", new IOException(se.getMessage(), se));
        assertEquals("test1", target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }

    @Test
    void constructServerException() {
        var target = new TsurugiIOException(new IceaxeServerExceptionTestMock("test", IceaxeErrorCode.TX_LOW_ERROR));
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR.name() + ": test", target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }

    @Test
    void constructCode() {
        var target = new IceaxeIOException(IceaxeErrorCode.TX_LOW_ERROR);
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR.getMessage(), target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }

    @Test
    void constructCodeIOException() {
        var se = new IceaxeServerExceptionTestMock("test", IceaxeErrorCode.TX_ALREADY_CLOSED);
        var target = new IceaxeIOException(IceaxeErrorCode.TX_LOW_ERROR, new IOException(se.getMessage(), se));
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR.getMessage() + ": " + se.getMessage(), target.getMessage());
        assertEquals(IceaxeErrorCode.TX_LOW_ERROR, target.getDiagnosticCode());
    }
}
