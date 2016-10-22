package com.poiitis.exceptions;

/**
 *
 * @author Marinos Poiitis
 */
public class ColumnNameMismatchException
extends AlgorithmExecutionException {
    private static final long serialVersionUID = -8252109115553025346L;

    public ColumnNameMismatchException(String message) {
        super(message);
    }

    public ColumnNameMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
