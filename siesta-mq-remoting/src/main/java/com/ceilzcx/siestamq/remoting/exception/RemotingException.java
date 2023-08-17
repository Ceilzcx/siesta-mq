package com.ceilzcx.siestamq.remoting.exception;

/**
 * @author ceilzcx
 * @since 8/12/2022
 */
public class RemotingException extends Exception {
    private static final long serialVersionUID = -5690687334570505110L;

    public RemotingException(String message) {
        super(message);
    }

    public RemotingException(String message, Throwable cause) {
        super(message, cause);
    }
}
