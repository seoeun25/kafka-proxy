package com.nexr.lean.kafka.util;

import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void testRandomString() {
        String r = Utils.randomString(8);
        Assert.assertEquals(8, r.length());
    }

}
