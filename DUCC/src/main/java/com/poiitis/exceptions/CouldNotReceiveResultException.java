package com.poiitis.exceptions;

/**
 *
 * @author Marinos Poiitis
 */
public class CouldNotReceiveResultException
extends AlgorithmExecutionException {
    private static final long serialVersionUID = -5581062620291673939L;

    public CouldNotReceiveResultException(String message) {
        super(message);
    }

    public CouldNotReceiveResultException(String message, Throwable cause) {
        super(message, cause);
    }
}
