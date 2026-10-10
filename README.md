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
#### 🖥️ Platform Support

Orclet runs on each *worker machine* in an Orca cluster. It targets 
*Windows machines* with *Docker Desktop (WSL2 backend)*.

### Recommended: Windows with WSL2

Docker Desktop on Windows uses WSL2 as its backend. Container 
operations run inside a WSL2 Linux VM.

*Setup:*

1. Install *Docker Desktop* for Windows — enable WSL2 backend
2. Install *Go 1.22+* inside WSL2 Ubuntu:
   ```bash
   cd ~
   wget https://go.dev/dl/go1.27.0.linux-amd64.tar.gz
   sudo tar -C /usr/local -xzf go1.27.0.linux-amd64.tar.gz
   export PATH=$PATH:/usr/local/go/bin
   go version


   ##Build and Run inside Wsl2 Ubuntu terminal
   cd ~/orca-project/orclet
go build -o orclet ./cmd/orclet
./orclet worker \
  --server=http://<DAEMON-IP>:9001 \
  --token=<TOKEN> \
  --name=node-1

  Windows Native (Advanced)
  $env:GOOS="windows"; $env:GOARCH="amd64"
go build -o orclet.exe ./cmd/orclet
.\orclet.exe worker --server=http://<DAEMON-IP>:9001 --token=<TOKEN> --name=node-1

#Running as a Windows Service (V2)
# Admin PowerShell
sc.exe create Orclet binPath= "C:\Tools\orclet.exe worker --config=C:\ProgramData\orclet\config.yaml" start= auto
sc.exe failure Orclet reset= 86400 actions= restart/5000/restart/5000/restart/5000
sc.exe start Orclet
sc.exe query Orclet

Window firewall rule(Admin PowerShell)
# On orclet machine — allow host port pool from daemon IP only
netsh advfirewall firewall add rule name="orca-pool" dir=in action=allow protocol=TCP localport=40000-41000 remoteip=<DAEMON-IP>

##Ports
Port Range Purpose
40000-41000 Host port pool for published container ports



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