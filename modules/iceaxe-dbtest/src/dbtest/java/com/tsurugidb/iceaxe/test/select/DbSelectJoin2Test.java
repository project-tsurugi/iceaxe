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
package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.IceaxeResultNameList.IceaxeAmbiguousNamePolicy;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select join test
 */
class DbSelectJoin2Test extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectJoin2Test.class);
        logInitStart(LOG, info);

        dropTable("a");
        dropTable("b");
        var session = getSession();
        executeDdl(session, "create table a (u int, v varchar(1))");
        executeDdl(session, "create table b (u int, v varchar(1))");

        var tm = createTransactionManagerOcc(session);
        tm.executeAndGetCountDetail("insert into a values (1, 'a'), (2, 'b'), (3, 'c')");
        tm.executeAndGetCountDetail("insert into b values (1, 'c'), (2, 'b'), (3, 'a')");

        logInitEnd(LOG, info);
    }

    @Test
    void join() throws Exception {
        var sql = "SELECT * FROM a JOIN b ON a.u = b.u AND a.v = b.v";

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);

        assertEquals(1, list.size());
        var entity = list.get(0);
        entity.setAmbiguousNamePolicy(IceaxeAmbiguousNamePolicy.FIRST);
        assertEquals(2, entity.getInt("u"));
        assertEquals("b", entity.getString("v"));
    }
}
