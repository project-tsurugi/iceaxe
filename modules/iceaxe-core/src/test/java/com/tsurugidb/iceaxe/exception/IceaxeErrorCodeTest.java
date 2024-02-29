package com.tsurugidb.iceaxe.exception;

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
        }
    }
}
