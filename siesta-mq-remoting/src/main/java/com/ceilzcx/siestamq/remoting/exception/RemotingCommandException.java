package com.ceilzcx.siestamq.remoting.exception;

/**
 * @author ceilzcx
 * @since 9/12/2022
 */
public class RemotingCommandException extends RemotingException {
    private static final long serialVersionUID = -6061365915274953096L;

    public RemotingCommandException(String message) {
        super(message, null);
    }

    public RemotingCommandException(String message, Throwable cause) {
        super(message, cause);
    }
}
