package com.tsurugidb.iceaxe.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Methods annotated with this annotation are intended for Iceaxe use only.
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR })
public @interface IceaxeInternal {

}
