<div id="top">

<!-- HEADER STYLE: CLASSIC -->
<div align="center">

<img src=".github/images/icon.png" alt="CarrierPigeons Logo" width="300"/>

# CarrierPigeons

**Continuous Telemetric Documentation Mesh & Distributed Execution Harness**  
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

‼ Please note that this project is very much in **active development** and work-in-progress. CarrierPigeons is currently undergoing design expansion into an importable SDK with Python API + `PigeonMath` analytics suite. Stay tuned!! :)



## Overview

</div>

**CarrierPigeons** is a lightweight, production-ready Continuous Telemetric Documentation pipeline that observes system metrics from edge devices and reliably delivers them to centralized storage via MQTT—complete with robust state management, batch processing, and graceful lifecycle handling. Included Runner system provides both out-of-the-box & extensively custom executable functions such as stress-testing & benchmarking nodes, integrated natively in the **CarrierPigeons** system.


**Why CarrierPigeons?**

This project empowers developers to build scalable, observable infrastructure monitoring systems without reinventing core pub/sub, storage, and resilience patterns. Transitioning beyond passive logging, CarrierPigeons acts as a proactive edge observability layer, heavily optimized for AI inference hardware, multi-node clusters, and Open Research environments. All managed through centralized CLI & TUI PigeonCommand tool. 

<br>

---

<div align="center">

## Features

</div>


🚀 **Pigeoneer Edge Agent**
* **Hardware-Agnostic Observers:** Auto-detects and independently observes Nvidia, AMD, Apple, and Intel GPUs alongside Linux/OpenWrt/macOS/Windows host, SoC thermal, CPU cooler fan speed (`cpu_fan_rpm`, `cpu_fan_pwm_pct`), and network telemetry. 
* **AI Accelerator Support:** Dedicated Hailo, Google Coral, & Apple ANE NPU support featuring PCIe device isolation and array scaling. 
* **State Awareness:** Event-driven heartbeat architecture utilizing MQTT Last Will and Testament (LWT) registries.

🏰 **PigeonCoop Ingestion**
* **Robust Storage:** PostgreSQL (`pgxpool`) powered CTD Ledger, Artifact, and Trace DAG persistence (`pkg/adapters/db`) with automated, zero-touch DDL schema migrations at startup. 
* **Event Watchdog:** Native Ntfy integration for instant webhook alerts on offline or degraded hardware nodes.

🧠 **Observability & AI** 
* **Pigeons:** Interactive CLI/TUI for live diagnostics, runner dispatch
* **PigeonCare** AI-assisted incident triage.
* **PigeonMCP Sidecar:** Standalone Model Context Protocol (MCP) server enabling tools like Ollama, Claude, ChatGPT, and more to query the CTD Ledger and Artifact Lineage DAG natively. 

⚙️ **Architecture**
* **Modular Go Monorepo:** Structured using standard `cmd/`, `internal/`, and `pkg/` layouts. 
* **Zero-Friction Deployment:** Compiles to static binaries with `CGO_ENABLED=0`, enabling clean cross-compilation for RISC-V (`riscv64`), ARM (`arm64`), and x86 (`amd64`) architectures.
* **Flexible Integration Interfaces**: Use as stand-alone out-of-the-box plug-and-play system, custom whitelabelable Go wrapper implementation, or **Pygeons** Python API (coming soon).


---

<div align="center">

## Pigeoneer Edge Footprint

</div>

> [!NOTE]
> The figures below are **theoretical engineering estimates** derived from binary analysis, runtime heap modeling, and initial local profiling. Hard, bare-metal verified telemetry data across target RISC-V, ARM64, and x86 edge hardware will be published as formal cluster validation completes.

Because `pigeoneer` links only the Go standard library and `paho.mqtt.golang` (excluding database drivers, CLI frameworks, and TUI dependencies), it is designed to run with minimal observer effect alongside latency-sensitive workloads:

### Memory & Binary Profile

| Metric / State | Estimated Footprint | Architectural Notes |
| :--- | :--- | :--- |
| **Static Binary (`CGO_ENABLED=0 -s -w`)** | **~5.9 – 6.3 MiB** | Zero dynamic `libc` or CGO dependencies across `riscv64`, `arm64`, and `amd64`. |
| **Steady-State RSS (Online)** | **~8 – 12 MiB** | Go runtime, mapped read-only text pages, persistent MQTT socket buffers, and default 4 MiB minimum heap target (`GOGC=100`). |
| **Pre-allocated Offline Spool** | **~40 KiB** | 5,000-slot pointer ring buffer (`chan *core.CTDPayload`) allocated at boot. |
| **Worst-Case RSS (Spool Saturated)** | **~16 – 22 MiB** | Full 5,000-event offline backlog (~3.5–7 hours of disconnected buffering at 5s intervals). Can be hard-capped on constrained routers via `GOMEMLIMIT=16MiB`. |

### CPU Profile (5s Poll Interval)

| Target Profile | Estimated CPU | Observer Mechanism |
| :--- | :--- | :--- |
| **Headless Linux / RISC-V SBCs** | **`< 0.05% – 0.15%` (1 hart)** | Zero subprocess forks; reads `/proc/loadavg`, `/proc/meminfo`, `/proc/net/dev`, `/sys/class/thermal`, `/sys/class/hwmon` (CPU cooler RPM & PWM), and Coral `/sys/class/apex` directly in-process. |
| **OpenWrt Routers** | **`~0.1% – 0.3%` (1 core)** | Two lightweight native C `ubus call` invocations per 5s window (`< 2 MiB` transient child RSS) plus in-process sysfs thermal/fan reads. |
| **Discrete GPU / Hailo NPU Nodes** | **`~0.5% – 2.5%` (1 core)** | Driven by vendor CLI subprocess startup (`nvidia-smi`, `rocm-smi`, `hailortcli`); Intel GPUs use a single persistent `intel_gpu_top` JSON stream (`~0.1% – 0.3%`). |
| **Active `benchmark` Runner** | **`100% × NumCPU()`** | On-demand synthetic stress workload (`sha256` burn) with near-zero heap allocation; completely idle unless explicitly dispatched. |

<br>

---

<div align="center">

## Project Structure

</div>

~~~sh
└── /
    ├── cmd/
    │   ├── pigeoncoop/   # The CTD ingestion & ledger engine
    │   ├── pigeoneer/    # The edge telemetry & runner agent
    │   ├── pigeonmcp/    # The Model Context Protocol (MCP) sidecar
    │   └── pigeons/      # The PigeonCommand CLI/TUI orchestrator
    ├── deployments/
    │   ├── compose/      # Local Docker Compose stack (Mosquitto, PostgreSQL, Ntfy)
    │   └── install-pigeoneer.sh
    ├── internal/
    │   ├── pigeoncoop/
    │   │   └── broker/     # MQTT subscriber, event watchdog, and LWT Ntfy registry
    │   ├── pigeoneer/
    │   │   ├── observers/  # Platform, cooling, & accelerator observers (Linux, OpenWrt, macOS, Windows, GPU, NPU)
    │   │   ├── relay/      # FIFO offline spooler, MQTT publisher, LWT, and runner dispatcher
    │   │   └── runners/    # Built-in remote execution workloads (benchmark)
    │   └── pigeons/
    │       ├── adapters/   # Multi-provider LLM & SQL tool adapters (Ollama, Claude, OpenAI, vLLM, llama.cpp)
    │       ├── cli/        # Cobra CLI subcommands (ps, list, node, coop, runner)
    │       └── tui/        # Bubble Tea PigeonCare+ interactive triage agent
    └── pkg/
        ├── adapters/db/    # BaseDB storage contract & PostgresStore (Ledger, Artifacts, Traces)
        ├── core/           # Canonical CTDPayload (Observer, Runner, Tracer), topics, and interfaces
        ├── observer/       # Extensible Observer registry and telemetry contract
        └── runner/         # Extensible remote Runner registry and control envelope
~~~


---

<div align="center">

<img src=".github/images/pigeon.gif" alt="pigeon.gif" width="30"/>
// Built by

</div>

```go
package drewvlabs // Drew Vandagriff — Dallas, TX

var Ethos = [5]string{
	"Simplicity in Architecture",
	"Efficiency in Engineering",
	"Purity in Science",
	"Audacity in Art",
	"Life in Logic",
}
```
