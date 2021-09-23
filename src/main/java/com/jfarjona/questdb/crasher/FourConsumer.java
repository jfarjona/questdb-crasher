/*
 * FourConsumer.java
 * 
 * 
 * Copyright (c) 1991-2021 Juan Felipe Arjona
 *
 * Copyrighted and proprietary code, all rights are reserved.
 * Reproduction, copying, or reuse without written approval is strictly forbidden.
 *
 * @author Juan F. Arjona
 *
 */

package com.jfarjona.questdb.crasher;

import java.util.Objects;

/**
 *
 * @author jfarjona
 *
 * @param <T>
 * @param <U>
 * @param <V>
 * @param <W>
 */
@FunctionalInterface
public interface FourConsumer<T, U, V, W> {
    /**
     * Performs this operation on the given arguments.
     *
     * @param t the first input argument
     * @param u the second input argument
     * @param v the third input argument
     * @param w the fourth input argument
     */
    void accept(T t, U u, V v, W w);

    /**
     * Returns a composed {@code BiConsumer} that performs, in sequence, this
     * operation followed by the {@code after} operation. If performing either
     * operation throws an exception, it is relayed to the caller of the
     * composed operation.  If performing this operation throws an exception,
     * the {@code after} operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@code BiConsumer} that performs in sequence this
     * operation followed by the {@code after} operation
     * @throws NullPointerException if {@code after} is null
     */
    default FourConsumer<T, U, V, W> andThen(FourConsumer<? super T, ? super U, ? super V, ? super W> after) {
        Objects.requireNonNull(after);
        return
            (l, r, s, t) -> {
                accept(l, r, s, t);
                after.accept(l, r, s, t);
            };
    }
}

/*
 * @(#)FourConsumer.java   21/09/21
 * 
 * Copyright (c) 1990-2021 Juan F Arjona
 *
 * Reproduction, use, copy or any action done with this code without written authorization from the author is forbidden and will be prosecuted to the maximum extent to the law.
 *
 *
 *
 */
