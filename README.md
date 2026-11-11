# 18-749 Milestone 4: Active and Passive Replication with Manual Recovery

Run the demo twice: once in active mode and once in passive mode.

- **Active:** All three replicas serve requests. Clients deliver the first reply and discard duplicates. A restarted replica receives a checkpoint and catches up before replying again.
- **Passive:** Only the primary serves requests. It sends periodic checkpoints to the backups. If it crashes, a backup takes over, and the old primary can return as a backup.
- **Both modes:** Each LFD monitors its local replica. The GFD sends membership updates to the Replication Manager (RM) and the clients. You manually restart failed replicas.

Keep the `server`, `client`, and `LFD` folders beside each other. Run every command below from the folder that contains all three.

## First-time setup

Run this once on every laptop:

```sh
python3 server/demo.py setup
```

Before the professor demo, run the automated test with all three real clients:

```sh
.venv/bin/python client/tests/integration_demo.py
```

A successful run prints:

```text
Milestone 4 active with real clients: PASS
Milestone 4 passive with real clients: PASS
```

## Who runs what

- **Coordinator laptop:** RM, GFD, C1, C2, and C3.
- **S1 laptop:** LFD1 and S1. S1 starts as primary in passive mode.
- **S2 laptop:** LFD2 and S2. S2 starts as a backup in passive mode.
- **S3 laptop:** LFD3 and S3. S3 starts as a backup in passive mode.

Write down these four reachable IP addresses before starting:

```text
GFD_IP = coordinator laptop
S1_IP  = S1 laptop
S2_IP  = S2 laptop
S3_IP  = S3 laptop
```

Each process gets its own terminal. Replace the IP placeholders in the commands below.

## Commands for the four-laptop demo

Start the components in this order. The commands show **active** mode. For the passive run, stop the active processes and repeat the sequence with `--mode passive` on **GFD, S1, S2, and S3**. The RM, LFD, and client commands stay the same.

### 1. Coordinator laptop: Start RM, then GFD

Terminal 1:

```sh
python3 server/demo.py rm
```

Wait for `RM: 0 members`.

Terminal 2:

```sh
python3 server/demo.py gfd --mode active
```

Wait for `GFD: 0 members` and confirm that RM logs the empty membership from GFD.

### 2. S1 laptop: Start LFD1

Terminal 1:

```sh
python3 server/demo.py lfd1 --gfd-host GFD_IP --replica-host S1_IP
```

### 3. S2 laptop: Start LFD2

Terminal 1:

```sh
python3 server/demo.py lfd2 --gfd-host GFD_IP --replica-host S2_IP
```

### 4. S3 laptop: Start LFD3

Terminal 1:

```sh
python3 server/demo.py lfd3 --gfd-host GFD_IP --replica-host S3_IP
```

Wait for all three LFDs to register and answer GFD heartbeats. Replica membership is still zero at this point.

### 5. S1 laptop: Start S1

Terminal 2:

```sh
python3 server/demo.py s1 --mode active --gfd-host GFD_IP
```

Wait for LFD1 to receive a heartbeat reply from S1 and for GFD and RM to list S1. In passive mode, wait for GFD to announce S1 as primary.

### 6. S2 laptop: Start S2

Terminal 2:

```sh
python3 server/demo.py s2 --mode active --gfd-host GFD_IP
```

Wait for GFD and RM to list S1 and S2.

### 7. S3 laptop: Start S3

Terminal 2:

```sh
python3 server/demo.py s3 --mode active --gfd-host GFD_IP
```

Wait for GFD and RM to list all three replicas and for each replica to log `i_am_ready=1`. In passive mode, S2 and S3 should log checkpoints from S1 with `my_state` and `checkpoint_count`. The default checkpoint interval is two seconds; set `--checkpoint 2` on each replica to make it explicit.

### 8. Coordinator laptop: Start all three clients

Terminal 3:

```sh
.venv/bin/python client/client.py --client_id C1 --gfd_url ws://GFD_IP:9000
```

Terminal 4:

```sh
.venv/bin/python client/client.py --client_id C2 --gfd_url ws://GFD_IP:9000
```

Terminal 5:

```sh
.venv/bin/python client/client.py --client_id C3 --gfd_url ws://GFD_IP:9000
```

The clients learn the replication mode and replica addresses from GFD. In active mode, they receive replies from all three replicas. In passive mode, only S1 replies initially.

## What to do during the professor demo

### Active replication: Crash and recover S2

1. Show GFD and RM listing three ready replicas. Pick one request in a client console and show three replies with the same state: one delivered and two discarded as duplicates.
2. Press **Ctrl+C in the S2 server terminal only**. Leave LFD2 running.
3. Show LFD2 detecting the crash and GFD and RM listing **S1 and S3**. Show all three clients continuing to get replies from the survivors.
4. Rerun the S2 command in its server terminal. Show LFD2 detecting a new boot ID and GFD and RM adding S2 back.
5. Show S2 starting with `i_am_ready=0`, receiving a checkpoint from a survivor, and reaching `i_am_ready=1`. Show clients receiving replies from S2 again and continuing to discard duplicates.

### Passive replication: Recover S2, then fail over S1

1. Show all three clients getting replies from **S1 only**. Show S1 sending checkpoints and S2 and S3 receiving them.
2. Press **Ctrl+C in the S2 server terminal only**. Show LFD2 reporting the crash, GFD and RM dropping to **S1 and S3**, and clients continuing to receive replies from S1.
3. Rerun the passive S2 command. Show S2 receiving a checkpoint from S1 and becoming ready again as a backup. Confirm membership is back to three and S1 is still primary.
4. Press **Ctrl+C in the S1 server terminal only**. Leave LFD1 running. Show RM selecting S2 and GFD announcing S2 as primary after it catches up.
5. Without restarting clients, show C1, C2, and C3 getting replies from S2. Show S3 receiving checkpoints from S2.
6. Rerun the passive S1 command. Show S1 receiving a checkpoint from S2 and rejoining as a backup. GFD and RM should return to three members while **S2 stays primary**.

## One-laptop practice

On Ubuntu or WSL with tmux installed, start a manual rehearsal:

```sh
python3 server/milestone4_tmux.py active
```

The launcher starts everything in order and opens four tmux windows: **Coordinator**, **S1**, **S2**, and **S3**. It uses four loopback IPs, `127.0.0.1` through `127.0.0.4`, with all three replicas on port 8001.

- Press **Ctrl+B**, release, then **W** to choose a window.
- In a replica window, press **Ctrl+B → ↓** to select the bottom server pane. Check its label before pressing **Ctrl+C**.
- After stopping the server, press **↑ → Enter** at the shell prompt to restart it with the same command.

To switch modes, press **Ctrl+B → D** to detach, then run:

```sh
python3 server/milestone4_tmux.py --stop
python3 server/milestone4_tmux.py passive
```

Use the same `--stop` command when finished. The loopback rehearsal tests crash and recovery behavior; it does not test connectivity between laptops.

## What each source file does

- `rm.py`: tracks membership reported by GFD and selects a passive primary.
- `gfd.py`: tracks replica membership and readiness, publishes the current primary, and orders requests.
- `replica.py`: runs a replica in active or passive mode and handles checkpoints and recovery.
- `../client/client.py`: sends requests to active replicas or the current passive primary.
- `../LFD/lfd.py`: monitors one replica and reports crashes and restarts to GFD.
- `milestone4_tmux.py`: starts a local rehearsal with four tmux windows.
- `tests/milestone4_demo.py`: tests server recovery and failover directly through the protocol.
- `../client/tests/integration_demo.py`: tests both modes with the real clients and LFDs.

## Network ports

- **TCP 9000 on the coordinator:** LFDs, replicas, and clients connect to GFD.
- **TCP 9001 on coordinator loopback:** GFD connects to RM on the same machine.
- **TCP 8001 on each replica laptop:** clients connect to replicas, and replicas connect to each other for checkpoint transfers. LFD heartbeats to replicas stay local.

Use reachable laptop IPs and omit `--local` for the four-laptop run. On the coordinator, you can check each replica before launching clients:

```sh
python3 server/demo.py check --gfd-host GFD_IP --replica-host S2_IP
```

Repeat with S1 and S3. If a check times out, verify the advertised IP and firewall rules from the coordinator.
