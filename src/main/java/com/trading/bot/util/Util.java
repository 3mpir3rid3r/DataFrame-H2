package com.trading.bot.util;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class Util {
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String getIpAddress(HttpServletRequest serverHttpRequest) {
        try {
            if (serverHttpRequest.getRemoteAddr().contains("0:0:0:0:0:0:0:1")) {
                return InetAddress.getLocalHost().getHostAddress();
            } else {
                return serverHttpRequest.getRemoteAddr();
            }
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public static double calPercentage(double f, double e) {
        return (Math.abs(f - e) / f) * 100;
    }

    public static double findMax(double... ds) {
        return Arrays.stream(ds).filter(d -> d >= 0).max().orElse(0);
    }

    public static double findMin(double... ds) {
        return Arrays.stream(ds).filter(d -> d >= 0).min().orElse(0);
    }

    public static double findAverage(double... ds) {
        return Arrays.stream(ds).filter(d -> d >= 0).average().orElse(0);
    }

    public static double findSum(double... ds) {
        return Arrays.stream(ds).filter(d -> d >= 0).sum();
    }

    public static double findDiff(double x, double y) {
        return Math.abs(x - y);
    }

}
