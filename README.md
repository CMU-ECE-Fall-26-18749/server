# 18-749 Milestone 3: Passive Replication

This version is intentionally limited to the Milestone 3 rubric:

- S1 is the primary.
- S2 and S3 are backups.
- Clients connect to all three replicas but send requests only to S1.
- S1 sends periodic checkpoints to S2 and S3.
- During the demo, kill S2 and then S3. Do not kill S1.
- There is no Replication Manager, primary failover, or recovery in Milestone 3.

Keep the `server`, `client`, and `LFD` folders beside each other. Run every
command below from the folder that contains all three folders.

## First-time setup

Run this once on every laptop:

```sh
python3 server/demo.py setup
```

Before the professor demo, run the automatic local test:

```sh
python3 server/demo.py test
```

A successful run ends with `"result": "PASS"`.

## Who runs what

You need four laptops for the official layout.

- **Coordinator laptop:** GFD, C1, C2, and C3.
- **S1 laptop:** LFD1 and primary replica S1.
- **S2 laptop:** LFD2 and backup replica S2.
- **S3 laptop:** LFD3 and backup replica S3.

Write down these four IP addresses before starting:

```text
GFD_IP = coordinator laptop
S1_IP  = S1 laptop
S2_IP  = S2 laptop
S3_IP  = S3 laptop
```

Every command below needs its own terminal window.

## Commands for the four-laptop demo

Start the components in this exact order.

### 1. Coordinator laptop

Terminal 1:

```sh
python3 server/demo.py gfd
```

Wait until it prints `GFD: 0 members`.

### 2. S1 laptop

Terminal 1:

```sh
python3 server/demo.py lfd1 --gfd-host GFD_IP --replica-host S1_IP
```

### 3. S2 laptop

Terminal 1:

```sh
python3 server/demo.py lfd2 --gfd-host GFD_IP --replica-host S2_IP
```

### 4. S3 laptop

Terminal 1:

```sh
python3 server/demo.py lfd3 --gfd-host GFD_IP --replica-host S3_IP
```

Wait until all three LFDs have registered with the GFD.

### 5. Start S1, the primary

On the S1 laptop, Terminal 2:

```sh
python3 server/demo.py s1 --backup-host S2_IP --backup-host S3_IP --checkpoint 2
```

### 6. Start S2 and S3, the backups

On the S2 laptop, Terminal 2:

```sh
python3 server/demo.py s2
```

On the S3 laptop, Terminal 2:

```sh
python3 server/demo.py s3
```

The GFD should print membership changing from 0 to 1 to 2 to 3. S1 should print
that its checkpoint channels connected to S2 and S3. S2 and S3 should repeatedly
print `Received checkpoint` with `my_state` and `checkpoint_count`.

### 7. Start all three clients

On the coordinator laptop, Terminals 2, 3, and 4:

```sh
python3 server/demo.py c1 --gfd-host GFD_IP
```

```sh
python3 server/demo.py c2 --gfd-host GFD_IP
```

```sh
python3 server/demo.py c3 --gfd-host GFD_IP
```

Each client should say it connected to S1, S2, and S3. Requests and replies must
name only S1 because S1 is the primary. S2 and S3 receive checkpoints but do not
process client requests.

## What to do during the professor demo

1. Show GFD membership containing S1, S2, and S3.
2. Show C1, C2, and C3 continuously receiving replies from S1.
3. Show S1 sending checkpoints and S2/S3 receiving them.
4. Press Ctrl+C only in the **S2 server terminal**. Leave LFD2 running.
5. Show LFD2 detecting the fault and GFD membership becoming S1, S3.
6. Show all clients continuing to receive replies from S1.
7. Show S1 continuing to send checkpoints to S3.
8. Press Ctrl+C only in the **S3 server terminal**. Leave LFD3 running.
9. Show membership becoming S1 and all clients continuing normally.

Do not stop S1. Primary failure and recovery are outside Milestone 3.

## One-laptop practice

Open ten terminals and run these commands in order:

```sh
python3 server/demo.py gfd
python3 server/demo.py lfd1 --local
python3 server/demo.py lfd2 --local
python3 server/demo.py lfd3 --local
python3 server/demo.py s1 --local --checkpoint 2
python3 server/demo.py s2 --local
python3 server/demo.py s3 --local
python3 server/demo.py c1
python3 server/demo.py c2
python3 server/demo.py c3
```

Each line still needs its own terminal. In local mode, S1/S2/S3 use ports
8001/8002/8003 on the same laptop.

## What each source file does

- `gfd.py`: tracks live replicas, publishes membership, and orders requests.
- `passive_server.py`: runs S1 as primary or S2/S3 as backups and checkpoints.
- `../client/client.py`: connects to all replicas and sends only to S1.
- `../LFD/lfd.py`: heartbeats one local replica and reports add/delete events.
- `tests/milestone3_demo.py`: runs the complete Milestone 3 scenario locally.

## Network ports

- GFD uses TCP port 9000 on the coordinator laptop.
- Every replica uses TCP port 8001 on its own laptop.
- LFD-to-replica heartbeat traffic stays on the same replica laptop.
- S1 must be able to reach S2 and S3 on port 8001.
- The coordinator must be able to reach S1, S2, and S3 on port 8001.

Keep all laptops awake and plugged in. If a connection fails, first check the IP
addresses, the host firewall, and whether all machines are on the same network.

No code has been pushed to GitHub. Review and push it yourself when ready.
