package com.tsurugi.iceaxe.statement;

import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet;
import com.nautilus_technologies.tsubakuro.protos.RequestProtos.ParameterSet.Parameter;

/**
 * Tsurugi Parameter for PreparedStatement
 * 
 * <p>
 * Do not check with {@link TgVariable}.
 * </p>
 */
public class TgParameterUncheck implements TgParameter {

    private final ParameterSet.Builder lowBuilder = ParameterSet.newBuilder();
    private ParameterSet lowParameterSet;

    /**
     * Tsurugi Parameter for PreparedStatement
     */
    public TgParameterUncheck() {
        // do nothing
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, int value) {
        var lowParameter = Parameter.newBuilder().setName(name).setInt4Value(value);
        add(lowParameter);
        return this;
    }

    /**
     * add value(int)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, Integer value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt4Value(value);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, long value) {
        var lowParameter = Parameter.newBuilder().setName(name).setInt8Value(value);
        add(lowParameter);
        return this;
    }

    /**
     * add value(long)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, Long value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setInt8Value(value);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, float value) {
        var lowParameter = Parameter.newBuilder().setName(name).setFloat4Value(value);
        add(lowParameter);
        return this;
    }

    /**
     * add value(float)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, Float value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat4Value(value);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, double value) {
        var lowParameter = Parameter.newBuilder().setName(name).setFloat8Value(value);
        add(lowParameter);
        return this;
    }

    /**
     * add value(double)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, Double value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setFloat8Value(value);
        }
        add(lowParameter);
        return this;
    }

    /**
     * add value(String)
     * 
     * @param name  name
     * @param value value
     * @return this
     */
    public TgParameterUncheck set(String name, String value) {
        var lowParameter = Parameter.newBuilder().setName(name);
        if (value != null) {
            lowParameter.setCharacterValue(value);
        }
        add(lowParameter);
        return this;
    }

    protected void add(Parameter.Builder lowParameter) {
        lowBuilder.addParameters(lowParameter.build());
        this.lowParameterSet = null;
    }

    // internal
    @Override
    public ParameterSet toLowParameterSet() {
        if (this.lowParameterSet == null) {
            this.lowParameterSet = lowBuilder.build();
        }
        return this.lowParameterSet;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + lowBuilder + "]";
    }
}
