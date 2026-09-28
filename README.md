# 18-749 Milestone 2

Active replication using the existing Python/WebSocket application. This repository
contains the replica, the new GFD, tests, and the launch helper. Keep the `server`,
`client`, and `LFD` repositories in sibling folders.

For repository setup, testing, and the rehearsal sequence, follow
[the step-by-step demo guide](DEMO_GUIDE.md).

## Quick start

From the folder containing those three folders, with Python 3.10 or newer:

```text
python server/demo.py setup
python server/demo.py test
```

On macOS/Linux, use `python3` if `python` is unavailable. Setup creates a local
`.venv`; activation is not needed. The test starts local processes, injects faults,
writes `server/test-results/report.json` and logs, and stops its own processes.
It does not contact the team's Vultr server. Dependencies are pinned to the versions
used during development. Python 3.13.2 on Windows was used for verification.

## Four-machine demonstration

The project guide says the replicas must be on three different physical machines.
Use three teammate laptops for S1/LFD1, S2/LFD2, S3/LFD3 and a fourth laptop for the
GFD and C1/C2/C3. VMs or containers on one laptop are useful for development but do
not provide that physical separation. Ask the TA before substituting cloud VMs.

Each machine gets the same three folders and runs setup once. Keep every process
in its own terminal. All commands below are run from the common parent folder.
Replace `GFD_IP` and `S1_IP` / `S2_IP` / `S3_IP` with actual reachable IPv4 addresses.
`127.0.0.1` on the LFD-to-replica link is intentional: those two run on the same laptop.

Network paths needed:

- LFD laptops -> coordinator TCP 9000.
- Coordinator -> each replica laptop TCP 8001.
- Use a network that permits laptop-to-laptop traffic. Being on the same Wi-Fi
  does not by itself prove reachability. Allow these Python connections/ports if
  the host firewall prompts; do not disable the whole firewall.
- Keep all four machines awake and plugged in during the demo.

Start in the rubric's order:

1. **Coordinator terminal 1:** `python server/demo.py gfd`
   Expect `GFD: 0 members`.
2. **Laptop S1, terminal 1:**
   `python server/demo.py lfd1 --gfd-host GFD_IP --replica-host S1_IP`
   Run equivalent commands with `lfd2`/`S2_IP` and `lfd3`/`S3_IP` on the other laptops.
   Expect LFD registration and GFD/LFD heartbeat logs; no replicas are members yet.
3. **Laptop S1, terminal 2:** `python server/demo.py s1`
   Then `python server/demo.py s2` on laptop S2 and `python server/demo.py s3` on S3.
   Expect GFD membership to grow 0 -> 1 -> 2 -> 3 after successful local heartbeats.
4. **Coordinator:** verify each laptop with
   `python server/demo.py check --replica-host S1_IP` (repeat for S2_IP and S3_IP).
5. **Coordinator terminals 2, 3, 4:** run respectively:
   `python server/demo.py c1`, `python server/demo.py c2`, `python server/demo.py c3`.
   Launch clients only after all three replicas appear at the GFD.

For one-laptop practice, add `--local` to each replica and LFD command, and omit
the IP flags. This uses replica ports 8001, 8002, 8003 on loopback. The automated
test uses its own available ports, so it will not kill an existing manual demo.

## Demo checklist

- [ ] GFD starts with zero members; three LFDs register before replicas start.
- [ ] GFD-to-LFD and LFD-to-server heartbeats are visible, with configurable intervals.
- [ ] Membership grows as S1, S2, S3 start; each client receives the full membership.
- [ ] Each client sends the same numbered request to all healthy replicas before
      moving to its next request. Requests and replies identify client/replica/number.
- [ ] All replicas process the same global order and print state before/after.
- [ ] Each client delivers only the first reply and logs late duplicates.
- [ ] Press Ctrl+C **only in S1's server terminal**. Keep LFD1 alive. Watch LFD1
      report the fault, GFD change to S2/S3, and every client continue.
- [ ] After the system settles, press Ctrl+C only in S2's server terminal.
      Watch membership become S3 and all clients continue, now without replica duplicates.
- [ ] Explain the assumptions and limitations below.

To repeat a clean demo, stop clients, LFDs, replicas, and GFD, then restart all of
them in the above order. Restarting one replica with an empty state during traffic
is deliberately rejected; recovery/state transfer belongs to later milestones.

## Design and protocol

The GFD maintains `membership[]`, a membership version, and a run epoch. Each LFD
registers a replica's externally reachable WebSocket URL, answers GFD heartbeats,
and reports add/delete transitions based on local replica heartbeats. GFD loss of
an LFD also removes its replica. A replica boot ID prevents accidental admission
of a fresh empty-state process after requests have begun.

The GFD additionally assigns a monotonically increasing `global_seq` to each
`(client_id, request_num)`. This is an explicit sequencing role co-located with the
already-allowed unreplicated GFD. It does not execute the application or relay
application replies: clients still send requests directly to all replicas.
TCP orders each connection, not the interleaving of different clients' connections,
so replicas buffer out-of-order requests until the missing sequence arrives.
This gives the counter the same state/result for a request at every replica.

Client sending and receiving are independent for each replica. The first reply is
delivered immediately. Late replies are matched by the reply's actual request ID,
validated, then discarded. Unacknowledged requests are replayed after a connection
loss. Replicas cache their original result to prevent a lost reply from causing a
second state update. An identical request retried at the GFD gets the same sequence.

Control messages:

| Connection | Messages |
| --- | --- |
| LFD -> GFD `/ws/lfd/LFD1` | `register {replica_id,url}`, `status {alive,boot_id}`, `heartbeat_reply {heartbeat_count}` |
| GFD -> LFD | `heartbeat {heartbeat_count}` |
| Client -> GFD `/ws/client/C1` | `order {request_num,payload}` |
| GFD -> client | `membership {epoch,version,members}`, `ordered {epoch,global_seq,client_id,request_num,payload}` |
| Client -> replica `/ws/client/C1` | The same ordered message to each replica |
| Replica -> client | `reply {epoch,global_seq,client_id,replica_id,request_num,my_state}` |

`GET /health` on the GFD/replicas is a read-only diagnostics endpoint.
The original single-server mode remains available via `server.py --mode standalone`
and `client.py --server_url ws://HOST:8001`; LFD without `--gfd_url` monitors locally.

## Scope and tradeoffs to explain

- GFD and clients must remain alive for the demo; GFD/sequencer is an accepted
  single point of failure. Global sequencing adds a coordinator round trip.
- Client IDs are unique for a run. Restart the entire demo after a client or GFD
  process restart; durable sessions, leader election, and GFD recovery are not implemented.
- All replicas start before client traffic. Fresh replica recovery, checkpointing,
  passive replication, and the RM are not part of this milestone.
- The demo keeps request/reply history in memory. This is not an indefinitely
  running production service: there is no durable storage or history compaction.
- A client crash after receiving a sequence can leave a sequence gap. Client
  crashes are outside the stated replica-crash demo model.
- Local tests prove behavior of separate processes. They do not verify the physical
  machines, firewall rules, latency, or reachability of the final deployment.

## Rollback and Git

Each repository has an annotated local tag `milestone-1-complete` preserving its
original baseline. Implementation work is on `milestone-2`, leaving the
original `master` commit available. From each clean repository, inspect the old code
with `git switch --detach milestone-1-complete`; return with `git switch milestone-2`.
This changes your checkout; it does not restart or redeploy running services.

Publishing commands, once the authenticated GitHub account has write access:

```text
git -C server push origin refs/tags/milestone-1-complete milestone-2
git -C client push origin refs/tags/milestone-1-complete milestone-2
git -C LFD push origin refs/tags/milestone-1-complete milestone-2
```

## Source requirements

- [Project guide](https://docs.google.com/document/d/1Ak8UT_oD9V5kxCQ8wkgjq97D_UdT3qHRVV6i9AHLKpc/edit)
- [Course schedule](https://docs.google.com/spreadsheets/d/1HFqoqMS9RamaIfHLtqAw5monMa05wvpGFIufjhBhH58/edit)
- [Team demo signup](https://docs.google.com/spreadsheets/d/1lLYHtFg4-KPqx2E1SUj30aR6QpuRMs0o-C4Lq9ZZsjk/edit)

As checked September 28, 2026: Milestone 2 is September 30 and Team 18 is listed at
6:15 p.m. This implementation follows the detailed checklist's two sequential
replica crashes (3 -> 2 -> 1), despite the shorter goals mentioning one crash.
