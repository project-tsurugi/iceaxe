package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * case expression test
 */
class DbCaseTest extends DbTestTableTester {

    private static final int SIZE = 25;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(LOG, info);
    }

    @Test
    void caseSwitch() throws Exception {
        var sql = "select bar," //
                + " case bar % 3" //
                + "     when 0 then 'a'" //
                + "     when 1 then 'b'" //
                + "     else 'c'" //
                + " end c0" //
                + "\nfrom " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(SIZE, list.size());

        for (var entity : list) {
            switch (entity.getInt("bar") % 3) {
            case 0:
                assertEquals("a", entity.getString("c0"));
                break;
            case 1:
                assertEquals("b", entity.getString("c0"));
                break;
            default:
                assertEquals("c", entity.getString("c0"));
                break;
            }
        }
    }

    @Test
    void caseSwitch_noElse() throws Exception {
        var sql = "select bar," //
                + " case bar % 3" //
                + "     when 0 then 'a'" //
                + "     when 1 then 'b'" //
                + " end c0" //
                + "\nfrom " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(SIZE, list.size());

        for (var entity : list) {
            switch (entity.getInt("bar") % 3) {
            case 0:
                assertEquals("a", entity.getString("c0"));
                break;
            case 1:
                assertEquals("b", entity.getString("c0"));
                break;
            default:
                assertNull(entity.getStringOrNull("c0"));
                break;
            }
        }
    }

    @Test
    void caseSwitch_differentType() throws Exception {
        var sql = "select bar," //
                + " case bar % 3" //
                + "     when 0 then   0" //
                + "     when 1 then 1.0" //
                + "     else        9e0" //
                + " end c0" //
                + "\nfrom " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(SIZE, list.size());

        for (var entity : list) {
            switch (entity.getInt("bar") % 3) {
            case 0:
                assertEquals(0, entity.getDouble("c0"));
                break;
            case 1:
                assertEquals(1, entity.getDouble("c0"));
                break;
            default:
                assertEquals(9, entity.getDouble("c0"));
                break;
            }
        }
    }

    @Test
    void caseIf() throws Exception {
        var sql = "select bar," //
                + " case when bar%15 = 0 then 'FizzBuzz' " //
                + "      when bar% 3 = 0 then 'Fizz'" //
                + "      when bar% 5 = 0 then 'Buzz'" //
                + "      else cast(bar as varchar)" //
                + " end c0" //
                + "\nfrom " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(SIZE, list.size());

        for (var entity : list) {
            String expected = getFizzBuzz(entity.getInt("bar"));
            assertEquals(expected, entity.getString("c0"));
        }
    }

    private String getFizzBuzz(int n) {
        if (n % 15 == 0) {
            return "FizzBuzz";
        } else if (n % 3 == 0) {
            return "Fizz";
        } else if (n % 5 == 0) {
            return "Buzz";
        } else {
            return Integer.toString(n);
        }
    }

    @Test
    void caseIf_noElse() throws Exception {
        var sql = "select bar," //
                + " case when bar%15 = 0 then 'FizzBuzz' " //
                + "      when bar% 3 = 0 then 'Fizz'" //
                + "      when bar% 5 = 0 then 'Buzz'" //
                + " end c0" //
                + "\nfrom " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(SIZE, list.size());

        for (var entity : list) {
            String expected = getFizzBuzz(entity.getInt("bar"));
            if (!Character.isDigit(expected.charAt(0))) {
                assertEquals(expected, entity.getString("c0"));
            } else {
                assertNull(entity.getStringOrNull("c0"));
            }
        }
    }

    @Test
    void caseIf_differntType() throws Exception {
        var sql = "select foo, bar," //
                + " case when bar%15 = 0 then  15 " //
                + "      when bar% 3 = 0 then 3.0" //
                + "      when bar% 5 = 0 then 5e0" //
                + "      else foo" //
                + " end c0" //
                + "\nfrom " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        var list = tm.executeAndGetList(sql);
        assertEquals(SIZE, list.size());

        for (var entity : list) {
            String expected = getFizzBuzz(entity.getInt("bar"));
            switch (expected) {
            case "FizzBuzz":
                assertEquals(15, entity.getDouble("c0"));
                break;
            case "Fizz":
                assertEquals(3, entity.getDouble("c0"));
                break;
            case "Buzz":
                assertEquals(5, entity.getDouble("c0"));
                break;
            default:
                assertEquals(entity.getInt("foo"), entity.getDouble("c0"));
                break;
            }
        }
    }

    @Test
    void updateSet() throws Exception {
        var sql = "update " + TEST + " set " //
                + "  bar = octet_length(zzz)," //
                + "  zzz = case when bar%15 = 0 then 'FizzBuzz' " //
                + "             when bar% 3 = 0 then 'Fizz'" //
                + "             when bar% 5 = 0 then 'Buzz'" //
                + "             else cast(bar as varchar)" //
                + "        end";

        var tm = createTransactionManagerOcc(getSession());
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            var old = createTestEntity(entity.getFoo());
            assertEquals(old.getZzz().getBytes(StandardCharsets.UTF_8).length, entity.getBar());
            String expected = getFizzBuzz(old.getBar().intValue());
            assertEquals(expected, entity.getZzz());
        }
    }

    @Test
    void deleteWhere() throws Exception {
        var sql = "delete from " + TEST //
                + " where case when foo <= 9 then case bar%3 when 0 then 1" //
                + "                                          when 1 then 2" //
                + "                                          else        9" //
                + "                               end" //
                + "                          else foo / 10" //
                + "       end = 1";

        var expectedSet = new HashSet<Integer>();
        var deleteSet = new HashSet<TestEntity>();
        for (int i = 0; i < SIZE; i++) {
            var entity = createTestEntity(i);
            int foo = entity.getFoo();
            if (foo <= 9) {
                if (entity.getBar() % 3 == 0) {
                    deleteSet.add(entity);
                } else {
                    expectedSet.add(foo);
                }
            } else {
                if (foo / 10 == 1) {
                    deleteSet.add(entity);
                } else {
                    expectedSet.add(foo);
                }
            }
        }

        var tm = createTransactionManagerOcc(getSession());
        int count = tm.executeAndGetCount(sql);
        assertEquals(deleteSet.size(), count);

        var list = selectAllFromTest();
        assertEquals(expectedSet.size(), list.size());
        for (var entity : list) {
            assertTrue(expectedSet.contains(entity.getFoo()));
        }
    }
}
