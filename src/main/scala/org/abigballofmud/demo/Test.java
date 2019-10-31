package org.abigballofmud.demo;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/10/21 12:29
 * @since 1.0
 */
public class Test {

    public static void main(String[] args) {
        System.out.println(LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Asia/Shanghai")));
    }
}
