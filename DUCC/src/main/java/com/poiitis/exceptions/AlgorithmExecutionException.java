package com.poiitis.exceptions;

import java.io.Serializable;

/**
 *
 * @author Marinos Poiitis
 */

public class AlgorithmExecutionException
extends Exception
implements Serializable {
    private static final long serialVersionUID = 7685236041703421431L;

    protected AlgorithmExecutionException() {
    }

    public AlgorithmExecutionException(String message) {
        super(message);
    }

    public AlgorithmExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
