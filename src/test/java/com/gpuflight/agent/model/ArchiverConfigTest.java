package com.gpuflight.agent.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ArchiverConfigTest {

    @Test
    void defaults_applied_whenNullRegionAndPrefix() {
        ArchiverConfig config = new ArchiverConfig(
            "http://minio:9000", "my-bucket", null, "ACCESS", "SECRET", null, false
        );
        assertEquals("nyc3", config.region());
        assertEquals("raw-events/", config.prefix());
    }

    @Test
    void explicit_region_notOverridden() {
        ArchiverConfig config = new ArchiverConfig(
            "http://s3.aws.com", "bucket", "us-west-2", "KEY", "SKEY", "custom/", true
        );
        assertEquals("us-west-2", config.region());
        assertEquals("custom/", config.prefix());
        assertTrue(config.deleteAfterUpload());
    }

    @Test
    void allFields_accessible() {
        ArchiverConfig config = new ArchiverConfig(
            "http://endpoint", "bucket1", "eu-1", "ak", "sk", "logs/", true
        );
        assertEquals("http://endpoint", config.endpoint());
        assertEquals("bucket1", config.bucket());
        assertEquals("ak", config.accessKey());
        assertEquals("sk", config.secretKey());
    }
}
