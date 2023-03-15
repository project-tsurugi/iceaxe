package com.tsurugidb.iceaxe.sql.parameter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.Parameters;

// internal
public final class IceaxeLowParameterUtil {

    private IceaxeLowParameterUtil() {
        // do nothing
    }

    public static Parameter create(@Nonnull String name, boolean value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable Boolean value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, int value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable Integer value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, long value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable Long value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, float value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable Float value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, double value) {
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable Double value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable BigDecimal value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable String value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable byte[] value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable boolean[] value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable LocalDate value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable LocalTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable LocalDateTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable OffsetTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable OffsetDateTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        return Parameters.of(name, value);
    }

    public static Parameter create(@Nonnull String name, @Nullable ZonedDateTime value) {
        if (value == null) {
            return Parameters.ofNull(name);
        }
        var offsetDateTime = value.toOffsetDateTime();
        return Parameters.of(name, offsetDateTime);
    }
}
