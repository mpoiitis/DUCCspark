package com.poiitis.exceptions;

/**
 *
 * @author Poiitis Marinos
 */

public class InputIterationException
extends AlgorithmExecutionException {
    private static final long serialVersionUID = -7979656967183896126L;

    public InputIterationException() {
    }

    public InputIterationException(String message) {
        super(message);
    }

    public InputIterationException(String message, Throwable cause) {
        super(message, cause);
    }
}

