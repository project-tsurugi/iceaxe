package com.tsurugidb.iceaxe.statement;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class TgParameterListTest {

    @Test
    void testOfTgParameterArray() {
        var foo = TgVariable.ofInt4("foo");
        var list = TgParameterList.of(foo.bind(123));

        assertParameterList(TgParameter.of("foo", 123), list);
    }

    private void assertParameterList(TgParameter expected, TgParameterList actual) {
        assertParameterList(List.of(expected), actual);
    }

    private void assertParameterList(List<TgParameter> expected, TgParameterList actual) {
        var expectedLow = expected.stream().map(TgParameter::toLowParameter).collect(Collectors.toList());
        var actualLow = actual.toLowParameterList();
        assertEquals(expectedLow, actualLow);
    }
}
