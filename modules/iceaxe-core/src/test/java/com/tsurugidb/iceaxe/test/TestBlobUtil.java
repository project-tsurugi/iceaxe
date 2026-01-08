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
package com.tsurugidb.iceaxe.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import com.tsurugidb.iceaxe.sql.type.TgBlob;
import com.tsurugidb.iceaxe.util.IceaxeCloseableSet;

public class TestBlobUtil {

    public static List<TgBlob> getBlobList(IceaxeCloseableSet closeableSet) {
        var set = closeableSet.getInternalSet();
        var list = new ArrayList<TgBlob>();
        for (var c : set) {
            list.add((TgBlob) c);
        }
        return list;
    }

    public static TgBlob getBlob1(IceaxeCloseableSet closeableSet) {
        var list = getBlobList(closeableSet);
        assertEquals(1, list.size());
        return list.get(0);
    }
}
