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
package com.tsurugidb.iceaxe.test.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.system.TsurugiSystemInfo;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * system info test
 */
class DbSystemInfoTest extends DbTestTableTester {

    @Test
    void getSystemInfo() throws Exception {
        var session = getSession();

        TsurugiSystemInfo systemInfo = session.getSystemInfo();
        {
            String name = systemInfo.getName();

            assertEquals("tsurugidb", name);
        }
        {
            String version = systemInfo.getVersion();

            String regex = "\\d+\\.\\d+\\.\\S+"; // 数値.数値.文字列
            assertTrue(Pattern.matches(regex, version));
        }
    }
}
