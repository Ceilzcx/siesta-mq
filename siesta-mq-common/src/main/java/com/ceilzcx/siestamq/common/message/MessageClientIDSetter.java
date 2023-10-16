package com.ceilzcx.siestamq.common.message;

import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;

import com.ceilzcx.siestamq.common.UtilAll;

/**
 * @author ceilzcx
 * @since 7/10/2023
 * 设置message id
 */
public class MessageClientIDSetter {

    private static final int LEN;
    private static final char[] FIX_STRING;
    private static final AtomicInteger COUNTER;
    private static long startTime;
    private static long nextStartTime;

    static {
        byte[] ip;
        try {
            ip = UtilAll.getIP();
        } catch (Exception e) {
            ip = createFakeIP();
        }
        LEN = ip.length + 2 + 4 + 4 + 2;
        ByteBuffer tempBuffer = ByteBuffer.allocate(ip.length + 2 + 4);
        tempBuffer.put(ip);
        tempBuffer.putShort((short) UtilAll.getPid());
        tempBuffer.putInt(MessageClientIDSetter.class.getClassLoader().hashCode());
        FIX_STRING = UtilAll.bytes2string(tempBuffer.array()).toCharArray();
        setStartTime(System.currentTimeMillis());
        COUNTER = new AtomicInteger(0);
    }

    private synchronized static void setStartTime(long millis) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(millis);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        startTime = cal.getTimeInMillis();
        cal.add(Calendar.MONTH, 1);
        nextStartTime = cal.getTimeInMillis();
    }

    public static String createUniqID() {
        char[] sb = new char[LEN * 2];
        System.arraycopy(FIX_STRING, 0, sb, 0, FIX_STRING.length);
        long current = System.currentTimeMillis();
        if (current >= nextStartTime) {
            setStartTime(current);
        }
        int diff = (int) (current - startTime);
        if (diff < 0 && diff > -1000_000) {
            // may cause by NTP
            diff = 0;
        }
        int pos = FIX_STRING.length;
        UtilAll.writeInt(sb, pos, diff);
        pos += 8;
        UtilAll.writeShort(sb, pos, COUNTER.getAndIncrement());
        return new String(sb);
    }

    public static byte[] createFakeIP() {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(System.currentTimeMillis());
        bb.position(4);
        byte[] fakeIP = new byte[4];
        bb.get(fakeIP);
        return fakeIP;
    }

    public static void setUniqID(final Message msg) {
        if (msg.getProperty(MessageConstants.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) == null) {
            msg.putProperty(MessageConstants.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, createUniqID());
        }
    }

    public static String getUniqID(final Message msg) {
        return msg.getProperty(MessageConstants.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }
}
