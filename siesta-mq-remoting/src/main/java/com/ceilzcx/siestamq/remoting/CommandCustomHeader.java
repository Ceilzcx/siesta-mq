package com.ceilzcx.siestamq.remoting;

import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;

/**
 * @author ceilzcx
 * @since 9/12/2022
 *
 * msg传输包括两部分: header和 body, 这就是header接口
 */
public interface CommandCustomHeader {
    void checkFields() throws RemotingCommandException;
}
