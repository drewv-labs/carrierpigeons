<div id="top">

<!-- HEADER STYLE: CLASSIC -->
<div align="center">

<img src=".github/images/icon.png" alt="CarrierPigeons Logo" width="300"/>

# CarrierPigeons

**Continuous Telemetric Documentation Mesh,<br>
& Distributed Execution Harness**<br>
*Release the Pigeons!*  

~ Built with ~ 

<img src="https://img.shields.io/badge/Go-00ADD8.svg?style=default&logo=Go&logoColor=white" alt="Go">
<img src="https://img.shields.io/badge/PostgreSQL-4169E1.svg?style=default&logo=postgresql&logoColor=white" alt="PostgreSQL">
<img src="https://img.shields.io/badge/MQTT-660066.svg?style=default&logo=mqtt&logoColor=white" alt="MQTT">

~ for ~

![Arch](https://img.shields.io/badge/Arch-riscv64%20|%20arm64%20|%20x86-blue)  
![OS](https://img.shields.io/badge/OS-UNIX%20|%20MacOS%20|%20OpenWrt%20|%20Windows-blue)  
![GPU](https://img.shields.io/badge/GPU-Nvidia%20|%20AMD%20|%20Apple%20|%20Intel-blue)  
![NPU](https://img.shields.io/badge/NPU-Hailo%20|%20Google_Coral%20|%20Apple_ANE-blue)  

</div>

<br>

---

<div align="center">

‼ Please note that this project is in **active development**

## Overview

</div>

**CarrierPigeons** is a lightweight, heterogeneous Continuous Telemetric Documentation (CTD) mesh for `riscv64`, `arm64`, and `amd64` architectures across Linux, OpenWrt, macOS, and Windows operating systems. It observes hardware metrics—CPU cooler fan RPM & PWM duty, SoC thermals, load, memory, network interfaces, GPUs, and NPUs—and streams immutable CTD wide events over MQTT into a high-throughput PostgreSQL ledger.

Transitioning beyond passive logging, CarrierPigeons pairs continuous hardware observation with an integrated **Runner** execution dispatch & monitoring, **PigeonMath** analytical view propagation, **PigeonCare** LLM-assisted diagnostics, and **PigeonMCP** integration for modern developer AI harnesses. Development of the **Pygeons** official [uv](https://docs.astral.sh/uv/) optimized Python API will begin after the core Pigeon kit is solid which will be published to PyPI.

<br>

---

<div align="center">

## Components & Capabilities

</div>

🚀 **Pigeoneer (`cmd/pigeoneer` / `pkg/edge`) — Configurable Edge Observer + Runner Daemon**
* **Self-Aware Hardware Detection:** Auto-detects RISC-V/ARM/x86 host sensors, CPU cooler RPM/PWM (`/sys/class/hwmon` & `cooling_device*`), OpenWrt zero-fork `/proc` & `/sys`, macOS SMC/ANE, Nvidia/AMD/Intel GPUs, and Hailo/Coral NPUs.
* **Variable Ring-Buffer Spool:** Configurable `spool_capacity` circular ring buffer evicts oldest frames in `O(1)` when `PigeonCoop` is unreachable without blocking observers or leaking heap memory.
* **Node Tagging:** Publishes hardware architecture and boot tags (`riscv64`, `compute-bench`, `npu-node`, etc.) on connect, merged with operator-assigned tags on the hub.

🏰 **PigeonCoop (`cmd/pigeoncoop` / `pkg/coop`) — Embedded Broker + Batched CTD Ledger Hub**
* **Zero-Dependency Embedded MQTT Broker:** Ships a pure-Go MQTT v3.1.1/v5.0 broker (`mochi-mqtt`, `CGO_ENABLED=0`) out of the box, with seamless configuration switch (`mqtt.mode = "external"`) for external Mosquitto clusters.
* **Hypertuned `COPY` Injection Engine:** Dual-trigger batch worker streams CTD wide events into PostgreSQL via binary `COPY FROM STDIN` (`pgx.CopyFrom`) with automatic poison-row fallback and actor attribution (`db_actor`, `app_actor`).
* **Implicit Self-Observation & Runners:** Every `PigeonCoop` natively observes its own host hardware and executes Runners using top-level `pigeoncoop.toml` settings—no separate edge daemon or duplicated config section required.
* **Selector Router & Ntfy Watchdog:** Resolves `RunnerRequestCTD` targets across `--all`, `--node`, and `--tag` unions, records provenance edges in `ctd_traces`, and dispatches Ntfy alerts on LWT state transitions.

🎛️ **Pigeons (`cmd/pigeons` / `pkg/controlplane`) — Workstation CLI & 4-Tab TUI Control Plane**
*Installed strictly on operator laptops/workstations to manage the mesh:*
* **Chained Runner Dispatch -> Live TUI:**
  ```sh
  pigeons run SyntheticComputeRnr --node=rv64-compute-01 --node=rv64-vision-02 --tag={compute-bench}
  ```
  Publishes a `RunnerRequestCTD` to `PigeonCoop` and boots directly into the interactive **`RunnerManager`** TUI tab.
* **Hub-Authoritative Node Tagging:**
  ```sh
  pigeons tag databroker rv64-coop-01
  ```
* **4-Tab Interactive TUI (`pigeons tui`):** `1:Fleet Overview`, `2:RunnerManager`, `3:PigeonMath`, and `4:PigeonCare`.

📐 **PigeonMath (`internal/pigeonmath`) — Analytics TUI & Certified `pmv_*` View Engine**
* **`btop`-Inspired TUI Telemetry Graphs:** Auto-refreshing 1-minute aggregated time-series plots tracking `#nodes` (`Total`, `Live`, `Quiet`) and `#CTD/min` (`Observer`, `Runner`, `Tracer`).
* **Certified View Propagation (`pigeonmath.pmv_*`):** Caches raw SQL intent in `pigeonmath.pmv_catalog` (preserving backtest ground truth across LLM context flushes), tunes SQL via `PigeonCare`, validates read-only planner execution (`EXPLAIN`), materializes `pigeonmath.pmv_<view_name>`, and certifies output integrity.

🩺 **PigeonCare & PigeonMCP (`cmd/pigeonmcp` / `pkg/mcpserver`) — Read-Only AI Diagnostics**
* **Hard Read-Only CTD Ledger Invariant:** `PigeonCare` and `PigeonMCP` connect through `db.AgentSafeDB` (`READ ONLY` PostgreSQL transactions + `pigeoncare_ro` RBAC role). Under no circumstances can an LLM agent write to or mutate public CTD Ledger tables; analytical output is strictly isolated to `pigeonmath.pmv_*` views.
* **Ephemeral Runners & Permission Modes:** `PigeonCare` can synthesize and deploy sandboxed `sh`/`bash`/`python3` diagnostic Runners governed by `[Allow, Always Allow, Deny]` confirmation prompts and **`[Default, Auto Edit, YOLO]`** autonomy modes (`Ctrl+P` in TUI).
* **Universal MCP Connectivity:** `pigeonmcp` serves `stdio` (default) and Streamable HTTP (`--http 127.0.0.1:8089`) for Zed, Google Antigravity, Claude Desktop/Code, and custom agent harnesses.

---

<div align="center">

## Pigeoneer Edge Footprint

</div>

> [!NOTE]
> The figures below are **theoretical engineering estimates** derived from binary analysis, runtime heap modeling, and initial local profiling. Hard, bare-metal verified telemetry data across target RISC-V, ARM64, and x86 edge hardware will be published as formal cluster validation completes.

Because `pigeoneer` links only the Go standard library, `BurntSushi/toml`, and `paho.mqtt.golang` (excluding database drivers, embedded brokers, and TUI frameworks), it runs with minimal observer effect alongside latency-sensitive workloads:

### Memory & Binary Profile

| Metric / State | Estimated Footprint | Architectural Notes |
| :--- | :--- | :--- |
| **Static Binary (`CGO_ENABLED=0 -s -w`)** | **~6.0 – 6.5 MiB** | Zero dynamic `libc` or CGO dependencies across `riscv64`, `arm64`, and `amd64`. |
| **Steady-State RSS (Online)** | **~8 – 12 MiB** | Go runtime, mapped read-only text pages, persistent MQTT socket buffers, and default 4 MiB minimum heap target (`GOGC=100`). |
| **Variable Offline Ring Spool** | **~8 KiB – 80 KiB** | Configurable `spool_capacity` (`1,000`–`10,000` slots) circular pointer ring buffer with `O(1)` oldest-event eviction. |
| **Worst-Case RSS (Spool Saturated)** | **~16 – 24 MiB** | Full offline backlog (~7–14 hours at 5s intervals). Can be hard-capped on constrained routers via `GOMEMLIMIT=16MiB` and `spool_capacity = 1000`. |

### CPU Profile (5s Poll Interval)

| Target Profile | Estimated CPU | Observer Mechanism |
| :--- | :--- | :--- |
| **Headless Linux / RISC-V SBCs** | **`< 0.05% – 0.15%` (1 hart)** | Zero subprocess forks; reads `/proc/loadavg`, `/proc/meminfo`, `/proc/net/dev`, `/sys/class/thermal`, `/sys/class/hwmon` (CPU cooler RPM & PWM), and Coral `/sys/class/apex` directly in-process. |
| **OpenWrt Routers** | **`< 0.05% – 0.15%` (1 core)** | Zero subprocess forks; reads `/proc/uptime`, `/proc/loadavg`, `/proc/meminfo`, `/proc/net/dev` (including `br-lan` bridges), and `/sys` thermal/fan sensors directly in-process. |
| **Discrete GPU / Hailo NPU Nodes** | **`~0.5% – 2.5%` (1 core)** | Driven by vendor CLI subprocess startup (`nvidia-smi`, `rocm-smi`, `hailortcli`); Intel GPUs use a single persistent `intel_gpu_top` JSON stream (`~0.1% – 0.3%`). |
| **Active `SyntheticComputeRnr` / `benchmark`** | **`100% × NumCPU()`** | On-demand synthetic stress workload (`sha256` burn) with near-zero heap allocation; completely idle unless explicitly dispatched. |

<br>

---

<div align="center">

<img src=".github/images/pigeon.gif" alt="pigeon.gif" width="30"/>
// Built by

</div>

```go
package drewvlabs // Indie Data Systems R&D

var Ethos = [5]string{
	"Simplicity in Architecture",
	"Efficiency in Engineering",
	"Purity in Science",
	"Audacity in Art",
	"Life in Logic",
}
```
