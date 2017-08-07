package com.ifeng.rocketdb;

/**
 * Created by lijs
 * on 2017/7/31.
 */
public class Constants {
    public static final int SUCCESS = 200;
    public static final int FAIL = 500;

    public static final byte KEY_TYPE = 1;
    public static final byte HASH_TYPE = 0;

    public static final int OPT_GET = 'g' + 'e' + 't';
    public static final int OPT_SET = 's' + 'e' + 't';
    public static final int OPT_DEL = 'd' + 'e' + 'l';
    public static final int OPT_HGET = 'h' + 'g' + 'e' + 't';
    public static final int OPT_HSET = 'h' + 's' + 'e' + 't';
    public static final int OPT_HDEL = 'h' + 'd' + 'e' + 'l';

    public static final byte[] RESPONSE_OK = "2\nok\n".getBytes();
    public static final byte[] RESPONSE_OK2 = "2\nok\n\n".getBytes();
    public static final byte[] RESPONSE_NOT_FOUND = "9\nnot_found\n\n".getBytes();
    public static final byte[] RESPONSE_FAIL = "4\nfail\n\n".getBytes();
}
