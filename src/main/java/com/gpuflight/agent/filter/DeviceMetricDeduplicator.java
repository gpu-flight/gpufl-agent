package com.gpuflight.agent.filter;

import com.gpuflight.agent.config.JsonSettings;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Filters device_metric_batch events to suppress duplicate rows where
 * GPU metrics haven't meaningfully changed.  This reduces database writes,
 * network traffic, and storage when the GPU is idle.
 *
 * <p>Per-device tracking ensures multi-GPU systems are handled correctly.
 * A liveness guarantee forces at least one row per device every
 * {@link #FORCE_EMIT_MS} milliseconds even when fully idle.
 */
public class DeviceMetricDeduplicator {

    private static final long FORCE_EMIT_MS = 60_000;  // 1 minute
    private static final int  POWER_THRESHOLD_PCT = 10; // ±10% change

    private final Map<Integer, PrevSample> prev = new HashMap<>();

    /**
     * Filters a raw {@code device_metric_batch} JSON string.
     *
     * @return filtered JSON with only changed rows, or {@code null} if
     *         every row was suppressed (caller should skip publishing).
     */
    public String filterBatch(String rawJson) {
        try {
            JsonNode root = JsonSettings.MAPPER.readTree(rawJson);
            if (!root.has("columns") || !root.has("rows")) return rawJson;

            JsonNode columnsNode = root.get("columns");
            List<String> columns = new ArrayList<>();
            for (JsonNode c : columnsNode) columns.add(c.asText());

            int iDeviceId = columns.indexOf("device_id");
            int iGpuUtil  = columns.indexOf("gpu_util");
            int iMemUtil  = columns.indexOf("mem_util");
            int iTempC    = columns.indexOf("temp_c");
            int iPowerMw  = columns.indexOf("power_mw");
            int iUsedMib  = columns.indexOf("used_mib");
            int iClockSm  = columns.indexOf("clock_sm");

            // If columns are missing, pass through unfiltered
            if (iDeviceId < 0 || iGpuUtil < 0) return rawJson;

            ArrayNode rows = (ArrayNode) root.get("rows");
            ArrayNode kept = JsonSettings.MAPPER.createArrayNode();
            long now = System.currentTimeMillis();

            for (JsonNode row : rows) {
                int deviceId = row.get(iDeviceId).asInt();
                int gpuUtil  = safe(row, iGpuUtil);
                int memUtil  = safe(row, iMemUtil);
                int tempC    = safe(row, iTempC);
                int powerMw  = safe(row, iPowerMw);
                long usedMib = safeLong(row, iUsedMib);
                int clockSm  = safe(row, iClockSm);

                PrevSample p = prev.get(deviceId);
                boolean emit;

                if (p == null) {
                    emit = true; // first sample for this device
                } else if (now - p.lastEmitMs > FORCE_EMIT_MS) {
                    emit = true; // liveness
                } else {
                    emit = gpuUtil != p.gpuUtil
                        || memUtil != p.memUtil
                        || Math.abs(tempC - p.tempC) > 1
                        || percentDiff(powerMw, p.powerMw) > POWER_THRESHOLD_PCT
                        || usedMib != p.usedMib
                        || clockSm != p.clockSm;
                }

                if (emit) {
                    kept.add(row);
                    prev.put(deviceId, new PrevSample(
                        gpuUtil, memUtil, tempC, powerMw, usedMib, clockSm, now));
                }
            }

            if (kept.isEmpty()) return null;
            if (kept.size() == rows.size()) return rawJson; // nothing filtered

            ((ObjectNode) root).set("rows", kept);
            return JsonSettings.MAPPER.writeValueAsString(root);

        } catch (Exception e) {
            // On any parse error, pass through unfiltered
            return rawJson;
        }
    }

    private static int safe(JsonNode row, int idx) {
        return idx >= 0 && idx < row.size() ? row.get(idx).asInt() : 0;
    }

    private static long safeLong(JsonNode row, int idx) {
        return idx >= 0 && idx < row.size() ? row.get(idx).asLong() : 0;
    }

    private static int percentDiff(int a, int b) {
        if (b == 0) return a == 0 ? 0 : 100;
        return Math.abs(a - b) * 100 / b;
    }

    private record PrevSample(
        int gpuUtil, int memUtil, int tempC, int powerMw,
        long usedMib, int clockSm, long lastEmitMs
    ) {}
}
