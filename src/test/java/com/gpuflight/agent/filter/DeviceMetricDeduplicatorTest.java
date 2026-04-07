package com.gpuflight.agent.filter;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DeviceMetricDeduplicatorTest {

    @Test
    void testFilterBatch_initialSampleEmitted() {
        DeviceMetricDeduplicator dedup = new DeviceMetricDeduplicator();
        String json = "{\"columns\":[\"device_id\",\"gpu_util\"],\"rows\":[[0,10]]}";
        String result = dedup.filterBatch(json);
        assertEquals(json, result);
    }

    @Test
    void testFilterBatch_duplicateSuppressed() {
        DeviceMetricDeduplicator dedup = new DeviceMetricDeduplicator();
        String json = "{\"columns\":[\"device_id\",\"gpu_util\"],\"rows\":[[0,10]]}";
        
        // First one emitted
        dedup.filterBatch(json);
        
        // Second one (identical) suppressed
        String result = dedup.filterBatch(json);
        assertNull(result);
    }

    @Test
    void testFilterBatch_changeEmitted() {
        DeviceMetricDeduplicator dedup = new DeviceMetricDeduplicator();
        String json1 = "{\"columns\":[\"device_id\",\"gpu_util\"],\"rows\":[[0,10]]}";
        String json2 = "{\"columns\":[\"device_id\",\"gpu_util\"],\"rows\":[[0,20]]}";
        
        dedup.filterBatch(json1);
        String result = dedup.filterBatch(json2);
        assertEquals(json2, result);
    }

    @Test
    void testFilterBatch_multipleDevices() {
        DeviceMetricDeduplicator dedup = new DeviceMetricDeduplicator();
        String json1 = "{\"columns\":[\"device_id\",\"gpu_util\"],\"rows\":[[0,10],[1,5]]}";
        
        // Both emitted initially
        dedup.filterBatch(json1);
        
        // Device 0 changes, Device 1 same
        String json2 = "{\"columns\":[\"device_id\",\"gpu_util\"],\"rows\":[[0,20],[1,5]]}";
        String result = dedup.filterBatch(json2);
        
        assertNotNull(result);
        assertTrue(result.contains("[0,20]"));
        assertFalse(result.contains("[1,5]"));
    }

    @Test
    void testFilterBatch_invalidJson_passedThrough() {
        DeviceMetricDeduplicator dedup = new DeviceMetricDeduplicator();
        String invalid = "{invalid}";
        assertEquals(invalid, dedup.filterBatch(invalid));
    }
}
