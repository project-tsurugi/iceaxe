package com.tsurugidb.iceaxe.statement;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;

import com.nautilus_technologies.tsubakuro.low.sql.Parameters;
import com.tsurugidb.jogasaki.proto.SqlRequest.Parameter;

// internal
public final class IceaxeLowParameterUtil {

    private IceaxeLowParameterUtil() {
        // do nothing
    }

    public static Parameter create(String name, boolean value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, Boolean value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, int value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, Integer value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, long value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, Long value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, float value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, Float value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, double value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, Double value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, BigDecimal value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, String value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, byte[] value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, boolean[] value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, LocalDate value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, LocalTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, Instant value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(String name, ZonedDateTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        var instant = value.toInstant();
        return Parameters.of(name, instant);
    }
}