package com.ceilzcx.siestamq.remoting.exception;

/**
 * @author ceilzcx
 * @since 8/12/2022
 */
public class RemotingTooMuchRequestException extends RemotingException {
    private static final long serialVersionUID = 4326919581254519654L;

    public RemotingTooMuchRequestException(String message) {
        super(message);
    }
}
