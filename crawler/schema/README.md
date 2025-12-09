GPUmon Crawler Output Schema

Overview
- Producer: gpumon/crawler (this component). It polls NVML for GPU/process stats and enriches with gpumon_client NDJSON logs when available.
- Transport: Each record is emitted as a single JSON object (one per message) to the configured IMetricsSender.
- Purpose: Define the stable contract between the crawler and downstream backend/ingestion.

Event routing
- Records contain a discriminator field metricType. Currently supported values:
  - "gpu" — aggregate, per-GPU snapshot over the sampling window
  - "process" — per-process view (optionally enriched from client logs)

Common fields
- timestamp: string — ISO‑8601 UTC timestamp (e.g., 2025-12-01T18:51:30Z) for the end of the sampling window
- hostname: string — host name of the machine running the crawler
- gpuId: string — GPU UUID (NVML) if known, otherwise "unknown" for orphaned process metrics
- gpuName: string — GPU product name if known, otherwise "unknown" for orphaned process metrics
- metricType: string — "gpu" or "process"

GPU metric record (metricType = "gpu")
- Semantics: Averages over the sampling window defined by the crawler’s loop configuration.
- Fields:
  - totalMemoryMiB: integer (>= 0)
  - usedTotalMemoryMiB: integer (>= 0)
  - freeMemoryMiB: integer (>= 0)
  - gpuUtilPercent: integer (0–100) — average of samples
  - memUtilPercent: integer (0–100) — average of samples
  - temperatureCelsius: integer — average GPU temperature (C)
  - powerMilliwatts: integer — average board power draw (mW)

Example (gpu):
{"timestamp":"2025-12-01T18:51:30Z","hostname":"trainer-01","gpuId":"GPU-abc-uuid","gpuName":"NVIDIA A100-PCIE-40GB","metricType":"gpu","totalMemoryMiB":40960,"usedTotalMemoryMiB":1234,"freeMemoryMiB":39726,"gpuUtilPercent":72,"memUtilPercent":35,"temperatureCelsius":62,"powerMilliwatts":165000}

Process metric record (metricType = "process")
- Semantics: Per‑process memory usage sampled via NVML, optionally enriched using events parsed from client logs in the same time window.
- Required fields:
  - pid: integer — process id
  - processName: string — process name from the OS (or "unknown")
  - processUsedMemoryMiB: integer (>= 0) — average of samples; 0 for orphaned client logs not matched in NVML
- Optional enrichment fields (from client logs, if available for pid):
  - appName: string — InitOptions::appName
  - kernelName: string — comma‑separated kernel names observed in the window
  - tag: string — last observed tag
  - scopeMemDeltaMiB: integer — sum of memory deltas reported by scope_end events

Example (process, matched with NVML):
{"timestamp":"2025-12-01T18:51:30Z","hostname":"trainer-01","gpuId":"GPU-abc-uuid","gpuName":"NVIDIA A100-PCIE-40GB","metricType":"process","pid":1234,"processName":"python","processUsedMemoryMiB":2048,"appName":"trainer","kernelName":"vectorAdd,reduceSum","tag":"epoch1","scopeMemDeltaMiB":256}

Example (process, orphaned — no NVML match):
{"timestamp":"2025-12-01T18:51:30Z","hostname":"trainer-01","gpuId":"unknown","gpuName":"unknown","metricType":"process","pid":4321,"processName":"trainer","processUsedMemoryMiB":0,"appName":"trainer","kernelName":"matmul","tag":"stage2","scopeMemDeltaMiB":128}

JSON Schema
- A machine‑readable schema for these records is provided at: crawler/schema/crawler_metrics.schema.json
- It uses JSON Schema draft‑07 and oneOf on the metricType discriminator.

Notes
- The exact field names and shapes are derived from crawler/GpuMonitor.cpp.
- Additional fields may be added in a backward‑compatible way; consumers should ignore unknown fields.
- Units:
  - Memory: MiB (mebibytes)
  - Power: mW (milliwatts)
  - Temperature: Celsius
  - Utilization: percent (0–100)
