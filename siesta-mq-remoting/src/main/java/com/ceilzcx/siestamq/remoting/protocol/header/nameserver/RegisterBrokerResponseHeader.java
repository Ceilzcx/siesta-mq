package com.ceilzcx.siestamq.remoting.protocol.header.nameserver;

import com.ceilzcx.siestamq.remoting.CommandCustomHeader;
import com.ceilzcx.siestamq.remoting.exception.RemotingCommandException;

/**
 * @author ceilzcx
 * @since 20/12/2022
 */
public class RegisterBrokerResponseHeader implements CommandCustomHeader {
    private String haServerAddr;
    private String masterAddr;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }
}
