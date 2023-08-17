package com.ceilzcx.siestamq.remoting.annotation;

import java.lang.annotation.*;

/**
 * @author ceilzcx
 * @since 16/12/2022
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.LOCAL_VARIABLE})
public @interface CFNotNull {
}
