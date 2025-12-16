package com.tsurugidb.iceaxe.test.system;

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
        String version = systemInfo.getVersion();

        String regex = "^\\d+\\.\\d+\\.\\S+$"; // 数値.数値.文字列
        assertTrue(Pattern.matches(regex, version));
    }
}
