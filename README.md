Orclet — Worker Agent for Orca

Lightweight worker agent that runs Docker containers on behalf of the Orca control plane.

"Orca decides, Orclet executes."

---

🎯 Overview

Orclet runs on every worker machine in an Orca cluster. It registers with the Orca daemon, sends heartbeats, polls for work, executes container assignments, and reports health and statistics.

✨ Responsibilities

· 📝 Registration — One-time registration with Orca
· 💓 Heartbeat — Every 5 seconds, reports liveness and per-replica state
· 📥 Work Loop — Polls Orca for assigned replicas every 5 seconds
· 🐳 Container Lifecycle — Create, start, stop, remove via Docker SDK
· 💚 Health Checks — Liveness + readiness probes (HTTP/TCP/exec)
· 📊 Stats Collection — Samples CPU and memory per container every 15s
· 🔁 Restart on Policy — Always/OnFailure/Never with backoff
· 🔄 Adoption — Reconnects to running containers after restart

---

🖥️ Platform Support

Orclet is designed for Windows-first deployment. Development and testing were done on WSL2 Ubuntu, but the primary deployment target is Windows machines with Docker Desktop.

Windows (Primary Target)

Prerequisites:

· Windows 10/11 with WSL2 enabled
· Docker Desktop for Windows (Linux containers mode)
· Go 1.22+ for Windows
· Git for Windows

Setup:

Install Go for Windows from https://go.dev/dl/ (Windows installer).
Install Git for Windows from https://git-scm.com/download/win.

Clone and build:

powershell
cd $env:USERPROFILE\Desktop
mkdir orca-project
cd orca-project

git clone https://github.com/fardeensayyed16/orca.git
git clone https://github.com/fardeensayyed16/orclet.git

cd orclet
$env:GOOS="windows"; $env:GOARCH="amd64"
go build -o orclet.exe ./cmd/orclet


Run the worker (Windows Terminal, foreground):

powershell
cd $env:USERPROFILE\Desktop\orca-project\orclet
.\orclet.exe worker --server=http://<DAEMON-IP>:9001 --token=<TOKEN> --name=node-1


Windows Service (v2)

For deployment as a Windows Service (Administrator PowerShell):

powershell
sc.exe create Orclet binPath= "C:\Tools\orclet.exe worker --config=C:\ProgramData\orclet\config.yaml" start= auto
sc.exe failure Orclet reset= 86400 actions= restart/5000/restart/5000/restart/5000
sc.exe start Orclet
sc.exe query Orclet


Config file at C:\ProgramData\orclet\config.yaml:

server: <DAEMON-IP>:9001
token: <TOKEN>
name: node-1
address: <THIS-WORKER-LAN-IP>
host_port_range: 40000-41000

Lock the config to SYSTEM + Administrators only:

powershell
icacls "C:\ProgramData\orclet\config.yaml" /inheritance:r /grant:r SYSTEM:F Administrators:F


Windows Firewall Rules

Run in Administrator PowerShell on the worker machine:

powershell
netsh advfirewall firewall add rule name="orca-pool" dir=in action=allow protocol=TCP localport=40000-41000 remoteip=<DAEMON-IP>


WSL2 / Linux (Development)

For development convenience, the same Go code builds and runs on WSL2 Ubuntu or native Linux.

bash
cd ~/orca-project/orclet
go build -o orclet ./cmd/orclet
./orclet worker --server=http://<DAEMON-IP>:9001 --token=<TOKEN> --name=node-1


Ports

Port Range Purpose
40000-41000 Host port pool for published container ports

---

🚀 Quick Start

Prerequisites

· Docker in Linux containers mode
· Static LAN IP or DHCP reservation
· Network connectivity to Orca daemon

Build

bash
go build -o orclet ./cmd/orclet


Run

bash
./orclet worker --server=http://<DAEMON-IP>:9001 --token=<TOKEN> --name=node-1 --address=<THIS-MACHINE-IP>


---

🛠️ Tech Stack

Layer Technology
Language Go 1.22+
Docker SDK github.com/moby/moby/client v0.6.0
Runtime API github.com/moby/moby/api v1.56.0
Error Handling github.com/containerd/errdefs

---

⚠️ Requirements

· Docker in Linux containers mode
· Static LAN IP or DHCP reservation
· Windows Firewall rule for host port pool (40000-41000) from daemon IP only
· Valid Orca daemon address and authentication token
· Network connectivity between the worker and the Orca daemon

---

📄 License

MIT License



