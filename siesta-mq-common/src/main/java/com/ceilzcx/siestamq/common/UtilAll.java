package com.ceilzcx.siestamq.common;

import org.apache.commons.validator.routines.InetAddressValidator;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.text.NumberFormat;
import java.util.Enumeration;
import java.util.function.Supplier;

/**
 * @author ceilzcx
 * @since 22/12/2022
 */
public class UtilAll {
    public static final String MULTI_PATH_SPLITTER = System.getProperty("rocketmq.broker.multiPathSplitter", ",");

    private final static char[] HEX_ARRAY;

    private final static int PID;

    static {
        HEX_ARRAY = "0123456789ABCDEF".toCharArray();
        Supplier<Integer> supplier = () -> {
            // format: "pid@hostname"
            String currentJVM = ManagementFactory.getRuntimeMXBean().getName();
            try {
                return Integer.parseInt(currentJVM.substring(0, currentJVM.indexOf('@')));
            } catch (Exception e) {
                return -1;
            }
        };
        PID = supplier.get();
    }

    public static int getPid() {
        return PID;
    }

    public static boolean isBlank(String str) {
        return str == null || str.isEmpty();
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
        }
    }

    public static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File file1 : files) {
                deleteFile(file1);
            }
            file.delete();
        }
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static String bytes2string(byte[] src) {
        char[] hexChars = new char[src.length * 2];
        for (int j = 0; j < src.length; j++) {
            int v = src[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static void writeInt(char[] buffer, int pos, int value) {
        char[] hexArray = HEX_ARRAY;
        for (int moveBits = 28; moveBits >= 0; moveBits -= 4) {
            buffer[pos++] = hexArray[(value >>> moveBits) & 0x0F];
        }
    }

    public static void writeShort(char[] buffer, int pos, int value) {
        char[] hexArray = HEX_ARRAY;
        for (int moveBits = 12; moveBits >= 0; moveBits -= 4) {
            buffer[pos++] = hexArray[(value >>> moveBits) & 0x0F];
        }
    }

    public static byte[] getIP() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            byte[] internalIP = null;
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    ip = inetAddresses.nextElement();
                    if (ip == null) {
                        continue;
                    }
                    byte[] address = ip.getAddress();
                    if (ip instanceof Inet4Address) {
                        if (address.length == 4) {
                            if (ipCheck(address)) {
                                if (!isInternalIP(address)) {
                                    return address;
                                } else if (internalIP == null || address[0] == (byte) 127) {
                                    // todo 这里代码的意思是, 如果是127打头的IP, 循环取最后一个, 为什么要这么做?
                                    internalIP = address;
                                }
                            }
                        }
                    } else if (ip instanceof Inet6Address) {
                        if (address.length == 16) {
                            if (ipV6Check(address)) {
                                if (!isInternalV6IP(ip)) {
                                    return address;
                                }
                            }
                        }
                    }
                }
            }
            if (internalIP != null) {
                return internalIP;
            } else {
                throw new RuntimeException("Can not get local ip");
            }
        } catch (Exception e) {
            throw new RuntimeException("Can not get local ip", e);
        }
    }

    private static boolean ipCheck(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }

        InetAddressValidator validator = InetAddressValidator.getInstance();
        return validator.isValidInet4Address(ipToIPv4Str(ip));
    }

    private static boolean ipV6Check(byte[] ip) {
        if (ip.length != 16) {
            throw new RuntimeException("illegal ipv6 bytes");
        }

        InetAddressValidator validator = InetAddressValidator.getInstance();
        return validator.isValidInet6Address(ipToIPv6Str(ip));
    }

    /**
     * 判断是否是内部ip, 通过网段判断
     */
    public static boolean isInternalIP(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }

        //10.0.0.0~10.255.255.255
        //172.16.0.0~172.31.255.255
        //192.168.0.0~192.168.255.255
        //127.0.0.0~127.255.255.255
        if (ip[0] == (byte) 10) {
            return true;
        } else if (ip[0] == (byte) 127) {
            return true;
        } else if (ip[0] == (byte) 172) {
            if (ip[1] >= (byte) 16 && ip[1] <= (byte) 31) {
                return true;
            }
        } else if (ip[0] == (byte) 192) {
            if (ip[1] == (byte) 168) {
                return true;
            }
        }
        return false;
    }

    public static boolean isInternalV6IP(InetAddress inetAddr) {
        // Site local ipv6 address: fec0:xx:xx...
        return inetAddr.isAnyLocalAddress() // Wild card ipv6
                || inetAddr.isLinkLocalAddress() // Single broadcast ipv6 address: fe80:xx:xx...
                || inetAddr.isLoopbackAddress() //Loopback ipv6 address
                || inetAddr.isSiteLocalAddress();
    }

    public static String ipToIPv4Str(byte[] ip) {
        if (ip.length != 4) {
            return null;
        }
        return new StringBuilder().append(ip[0] & 0xFF).append(".").append(
                        ip[1] & 0xFF).append(".").append(ip[2] & 0xFF)
                .append(".").append(ip[3] & 0xFF).toString();
    }

    public static String ipToIPv6Str(byte[] ip) {
        if (ip.length != 16) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ip.length; i++) {
            String hex = Integer.toHexString(ip[i] & 0xFF);
            if (hex.length() < 2) {
                sb.append(0);
            }
            sb.append(hex);
            if (i % 2 == 1 && i < ip.length - 1) {
                sb.append(":");
            }
        }
        return sb.toString();

    }

}