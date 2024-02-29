package com.tsurugidb.iceaxe.exception;

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
            if (name.contains("CLOSE")) {
                assertTrue(message.contains("close"), "not contains 'close'. name=" + name);
            }
            if (name.contains("CHILD")) {
                assertTrue(message.contains("child resource"), "not contains 'child resource'. name=" + name);
            }
            if (name.contains("TIMEOUT")) {
                assertTrue(message.contains("timeout"), "not contains 'timeout'. name=" + name);
            }
            if (name.contains("ERROR")) {
                assertTrue(message.contains("error"), "not contains 'error'. name=" + name);
            }
        }
    }
}
