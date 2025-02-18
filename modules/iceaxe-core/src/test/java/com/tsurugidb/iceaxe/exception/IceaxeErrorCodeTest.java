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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.text.MessageFormat;
import java.util.HashMap;

import org.junit.jupiter.api.Test;

class IceaxeErrorCodeTest {

    @Test
    void checkCodeNumber() {
        var map = new HashMap<Integer, IceaxeErrorCode>();
        for (var code : IceaxeErrorCode.values()) {
            Integer codeNumber = code.getCodeNumber();
            if (map.containsKey(codeNumber)) {
                fail(MessageFormat.format("duplicate codeNumber {0}. {1}, {2}", codeNumber, map.get(codeNumber).name(), code.name()));
            }
            map.put(codeNumber, code);

            String name = code.name();
            String message = code.getMessage();
            if (name.contains("CONNECT")) {
                assertTrue(message.contains("connect"), "not contains 'connect'. name=" + name);
            }
            if (name.contains("SHUTDOWN")) {
                assertTrue(message.contains("shutdown"), "not contains 'shutdown'. name=" + name);
            }
            if (name.contains("CLOSE")) {
                assertTrue(message.contains("close"), "not contains 'close'. name=" + name);
            }
            if (name.contains("CHILD")) {
                assertTrue(message.contains("child resource"), "not contains 'child resource'. name=" + name);
            }
            if (name.contains("TIMEOUT")) {
                assertTrue(message.contains("timeout"), "not contains 'timeout'. name=" + name);
                assertTrue(code.isTimeout());
            } else {
                assertFalse(code.isTimeout());
            }
            if (name.contains("ERROR")) {
                assertTrue(message.contains("error"), "not contains 'error'. name=" + name);
            }
        }
    }
}
