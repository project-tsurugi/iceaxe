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
package com.tsurugidb.iceaxe.sql.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.result.IceaxeResultNameList.IceaxeAmbiguousNamePolicy;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.Column;

class IceaxeResultNameListTest {

    @Test
    void of() {
        var lowColumnList = List.of(column("foo", AtomType.INT4), column("bar", AtomType.INT8), column("zzz", AtomType.CHARACTER));
        var actual = IceaxeResultNameList.of(lowColumnList);

        assertEquals(lowColumnList.size(), actual.size());
        assertEquals(List.of("foo", "bar", "zzz"), actual.getNameList());
        assertEquals("foo", actual.getName(0));
        assertEquals("bar", actual.getName(1));
        assertEquals("zzz", actual.getName(2));
        assertEquals(0, actual.getIndex("foo", IceaxeAmbiguousNamePolicy.ERROR));
        assertEquals(1, actual.getIndex("bar", IceaxeAmbiguousNamePolicy.ERROR));
        assertEquals(2, actual.getIndex("zzz", IceaxeAmbiguousNamePolicy.ERROR));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3, 4, 5, 6, 7 })
    void toNameList(int pattern) {
        var name = new String[3];
        name[0] = ((pattern & 0b001) == 0) ? "" : "foo";
        name[1] = ((pattern & 0b010) == 0) ? "" : "bar";
        name[2] = ((pattern & 0b100) == 0) ? "" : "zzz";
        var lowColumnList = List.of(column(name[0], AtomType.INT4), column(name[1], AtomType.INT8), column(name[2], AtomType.CHARACTER));
        var nameList = IceaxeResultNameList.toNameList(lowColumnList);

        assertEquals(name.length, nameList.size());
        for (int i = 0; i < name.length; i++) {
            assertEquals(name[i].isEmpty() ? "@#" + i : name[i], nameList.get(i));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "ERROR", "FIRST", "LAST" })
    void getIndexPolicy(String policy0) {
        var policy = IceaxeAmbiguousNamePolicy.valueOf(policy0);

        var lowColumnList = List.of( //
                column("foo", AtomType.INT4), column("bar", AtomType.INT8), column("zzz", AtomType.CHARACTER), //
                column("foo", AtomType.INT4), column("bar", AtomType.INT8), column("foo", AtomType.CHARACTER) //
        );
        var target = IceaxeResultNameList.of(lowColumnList);

        assertEquals(List.of("foo", "bar", "zzz", "foo", "bar", "foo"), target.getNameList());
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("not-found", policy);
        });
        switch (policy) {
        case ERROR:
            assertThrows(IllegalArgumentException.class, () -> {
                target.getIndex("foo", policy);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                target.getIndex("bar", policy);
            });
            break;
        case FIRST:
            assertEquals(0, target.getIndex("foo", policy));
            assertEquals(1, target.getIndex("bar", policy));
            break;
        case LAST:
            assertEquals(5, target.getIndex("foo", policy));
            assertEquals(4, target.getIndex("bar", policy));
            break;
        default:
            throw new AssertionError(policy);
        }
        assertEquals(2, target.getIndex("zzz", policy));
    }

    @Test
    void getIndexSub() {
        var lowColumnList = List.of( //
                column("foo", AtomType.INT4), column("bar", AtomType.INT8), column("zzz", AtomType.CHARACTER), //
                column("foo", AtomType.INT4), column("bar", AtomType.INT8), column("foo", AtomType.CHARACTER) //
        );
        var target = IceaxeResultNameList.of(lowColumnList);

        assertEquals(List.of("foo", "bar", "zzz", "foo", "bar", "foo"), target.getNameList());
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("not-found", 0);
        });

        assertEquals(0, target.getIndex("foo", 0));
        assertEquals(3, target.getIndex("foo", 1));
        assertEquals(5, target.getIndex("foo", 2));
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("foo", 3);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("foo", -1);
        });

        assertEquals(1, target.getIndex("bar", 0));
        assertEquals(4, target.getIndex("bar", 1));
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("bar", 2);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("bar", -1);
        });

        assertEquals(2, target.getIndex("zzz", 0));
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("zzz", 1);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            target.getIndex("zzz", -1);
        });
    }

    @Test
    void multiThread() throws Exception {
        int columnSize = 100;
        int threadSize = 16;

        var lowColumnList = new ArrayList<Column>(columnSize);
        for (int i = 0; i < columnSize - 1; i++) {
            lowColumnList.add(column("c" + i, AtomType.INT4));
        }
        lowColumnList.add(column("c1", AtomType.INT8));

        var service = Executors.newCachedThreadPool();
        try {
            for (int attempt = 0; attempt < 10; attempt++) {
                var target = IceaxeResultNameList.of(lowColumnList);

                var startWait = new AtomicBoolean(true);

                var futureList = new ArrayList<Future<?>>(threadSize);
                for (int t = 0; t < threadSize; t++) {
                    var future = service.submit(() -> {
                        while (startWait.get()) {
                        }

                        for (int i = 0; i < columnSize - 1; i++) {
                            assertEquals(i, target.getIndex("c" + i, IceaxeAmbiguousNamePolicy.FIRST));
                        }
                        assertEquals(columnSize - 1, target.getIndex("c1", IceaxeAmbiguousNamePolicy.LAST));
                    });
                    futureList.add(future);
                }

                startWait.set(false);

                for (var future : futureList) {
                    future.get();
                }
            }
        } finally {
            service.shutdownNow();
        }
    }

    private static Column column(String name, AtomType type) {
        return Column.newBuilder().setName(name).setAtomType(type).build();
    }
}
