Orclet — Worker Agent for Orca

Lightweight worker agent that runs Docker containers on behalf of the Orca control plane.

«"Orca decides, Orclet executes."»

---

🎯 Overview

Orclet runs on every worker machine in an Orca cluster. It registers with the Orca daemon, sends heartbeats, polls for work, executes container assignments, and reports health and statistics.

✨ Responsibilities

- 📝 Registration — One-time registration with Orca.
- 💓 Heartbeat — Reports liveness and per-replica state every 5 seconds.
- 📥 Work Loop — Polls Orca for assigned replicas every 5 seconds.
- 🐳 Container Lifecycle — Creates, starts, stops, and removes containers via the Docker SDK.
- 💚 Health Checks — Supports liveness and readiness probes (HTTP/TCP/exec).
- 📊 Stats Collection — Samples CPU and memory usage per container every 15 seconds.
- 🔁 Restart Policies — Supports Always, OnFailure, and Never with backoff.
- 🔄 Adoption — Reconnects to running containers after a restart.

---

🚀 Quick Start

1. Build

From the Orclet project directory:

go build -o orclet ./cmd/orclet

2. Run

Start the worker agent:

./orclet worker \
  --server=http://<DAEMON-IP>:9001 \
  --token=<TOKEN> \
  --name=node-1 \
  --address=<THIS-MACHINE-IP>

Replace the placeholders with your actual Orca daemon IP, authentication token, and worker machine IP.

3. Run as a Windows Service

For Windows Service deployment (v2), run PowerShell as Administrator.

Create the service:

sc.exe create Orclet binPath= "C:\Tools\orclet.exe worker --config=C:\ProgramData\orclet\config.yaml" start= auto

Configure automatic restarts:

sc.exe failure Orclet reset= 86400 actions= restart/5000/restart/5000/restart/5000

Start the service:

sc.exe start Orclet

Note: Ensure that "orclet.exe" exists at the specified path and that the configuration file is present. Verify that the worker command supports the "--config" flag before deploying it as a Windows Service.

---

🛠️ Tech Stack

Layer| Technology
Language| Go 1.22+
Docker SDK| "github.com/moby/moby/client" v0.6.0
Runtime API| "github.com/moby/moby/api" v1.56.0
Error Handling| "github.com/containerd/errdefs"

---

⚠️ Requirements

- Docker configured for Linux containers.
- Static LAN IP address or DHCP reservation for the worker.
- Windows Firewall rules allowing the required host port range ("40000–41000") from the Orca daemon IP only.
- Valid Orca daemon address and authentication token.
- Network connectivity between the worker and the Orca daemon.

---

📄 License

This project is licensed under the MIT License.