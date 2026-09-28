# Milestone 2: GitHub setup and team rehearsal

Use the `milestone-2` branch in **all three repositories**. The default branch,
`master`, still contains Milestone 1. 

The implementation and local fault-injection tests are complete. A rehearsal on
the team's four physical machines is still required. Each person can do the
independent test below from anywhere before that rehearsal.

As checked September 28, 2026, the [demo signup](https://docs.google.com/spreadsheets/d/1lLYHtFg4-KPqx2E1SUj30aR6QpuRMs0o-C4Lq9ZZsjk/edit)
lists Team 18 for **Wednesday, September 30, at 6:15 p.m.** Check course
announcements for the room and any changes.

## 1. Message to send to the group now

> Milestone 2 is pushed to the milestone-2 branches in server, client, and LFD.
> The local tests passed, including continuing after two replica crashes. Please
> follow this guide to clone all three repos and run the independent test on your
> laptop. Send your OS and PASS/FAIL result, plus the terminal error if it fails.
> You can do this remotely without connecting to my laptop.
>
> Guide: https://github.com/CMU-ECE-Fall-26-18749/server/blob/milestone-2/DEMO_GUIDE.md
>
> We then need a four-laptop rehearsal. I can run GFD and the three clients.
> Please volunteer for S1/LFD1, S2/LFD2, or S3/LFD3, and send your availability
> for a rehearsal before Wednesday. Also, let's confirm who is the M2 project
> manager/presenter.

## 2. Teammates: check Git and Python

Windows: open **PowerShell** and run:

```powershell
git --version
python --version
```

macOS/Linux: open **Terminal** and run:

```bash
git --version
python3 --version
```

Python must be 3.10 or newer. If a command is missing or the Python version is
older, resolve that before proceeding. Send the exact output and OS if you need
help. All commands below use `python`; macOS/Linux users should replace it with
`python3` if that is the command available on their system.

## 3. Teammates: clone a separate copy for the demo

For first-time setup, run these commands **one line at a time**. They work in
PowerShell and macOS/Linux terminals. Use this new folder even if you already
have a Milestone 1 checkout elsewhere; it keeps that existing checkout intact.

```text
cd ~
mkdir 18749-m2-demo
cd 18749-m2-demo
git clone --branch milestone-2 https://github.com/CMU-ECE-Fall-26-18749/server.git server
git clone --branch milestone-2 https://github.com/CMU-ECE-Fall-26-18749/client.git client
git clone --branch milestone-2 https://github.com/CMU-ECE-Fall-26-18749/LFD.git LFD
```

If `18749-m2-demo` already exists, do not clone over existing folders. If it is a
previous clone of this setup, use section 13. Otherwise choose another new folder
name and use that name in the later `cd` commands.

Stop if any clone fails. When all three finish, the folder layout must be:

```text
18749-m2-demo/
  server/
  client/
  LFD/
```

Verify the branches:

```text
git -C server branch --show-current
git -C client branch --show-current
git -C LFD branch --show-current
```

All three must print `milestone-2`. Stay in `18749-m2-demo` when running the
remaining commands; do not change into `server` itself.

Tianyi's existing working folder is already prepared. There is no need for him
to clone a second copy; use the existing parent folder containing the three repos.

## 4. Teammates: install dependencies and run the independent test

Windows:

```powershell
python server/demo.py setup
python server/demo.py test
```

macOS/Linux:

```bash
python3 server/demo.py setup
python3 server/demo.py test
```

Run setup first and wait for it to finish. It creates a local `.venv`, installs
the required packages, and prints `Ready`. No manual environment activation is
needed. Then run the test and let it finish without pressing Ctrl+C.

Expected: the five unittest checks finish with `OK`, and the integration report
prints `PASS`. The test runs all components on that laptop and stops its own
processes afterward. It requires no teammate connection or Vultr access. It does
not replace the distributed rehearsal.

Send a result message like this:

```text
OS: Windows / macOS / Linux
Python version: ...
Branches: milestone-2 in all three repos
Setup: PASS / FAIL
Test: PASS / FAIL
Available for rehearsal: ...
Preferred role: S1 / S2 / S3 / presenter
```

If it fails, copy the terminal output starting at the error through the end.
Include any files created under `server/test-results`, especially `report.json`
and the component log for the error. On Windows, the report can be read with
`Get-Content server/test-results/report.json`; on macOS/Linux, use
`cat server/test-results/report.json`. An early failure may produce no report.

## 5. Coordinator: assign the four machines and presenter

Fill this table and send the completed version to the group:

| Role | Person | Processes | Reachable IPv4 address |
| --- | --- | --- | --- |
| Coordinator | Tianyi | GFD, C1, C2, C3 | GFD_IP = ... |
| Replica 1 | ... | LFD1, S1 | S1_IP = ... |
| Replica 2 | ... | LFD2, S2 | S2_IP = ... |
| Replica 3 | ... | LFD3, S3 | S3_IP = ... |

The fifth teammate can present or observe logs. The team chooses the rotating
milestone project manager; that person presents and answers questions while the
team attends. Agree on a rehearsal time before the demo.

## 6. Everyone: prepare the network and terminals

For the simplest rehearsal, meet with the four laptops on the intended demo
network. Plug them in, keep them awake, and leave the lids open. Being on the same
Wi-Fi does not guarantee that the network permits laptop-to-laptop traffic.

On Windows, run `ipconfig` and find the active adapter's IPv4 address. On
macOS/Linux, find the active connection's IPv4 address in network settings.
Report that address to the coordinator. Do not use `127.0.0.1` as another
laptop's address.

Required connections:

- Each LFD laptop to the coordinator: TCP 9000.
- Coordinator to each replica laptop: TCP 8001.

Allow the required Python connections on the network used for the demo if the
firewall prompts; keep the firewall enabled. Connectivity checks appear below.
Across different locations, a private network must first provide those same
connections. That remote network setup has not yet been verified. The independent
tests above can run remotely without it.

Each replica owner needs **two terminals**. The coordinator needs **four running
terminals**, plus a spare terminal for setup/checks if convenient. In every new
teammate terminal, first run:

```text
cd ~/18749-m2-demo
```

Tianyi uses his existing working folder instead. Every terminal's working folder
must directly contain `server`, `client`, and `LFD`.

Replace `GFD_IP`, `S1_IP`, `S2_IP`, and `S3_IP` below with the numbers from the
completed role table. They are placeholders, not literal hostnames. For example,
if the coordinator is `192.168.1.20`, use `--gfd-host 192.168.1.20`. That address
is only an example; use the actual address reported for the coordinator.

## 7. Start the GFD and register the LFDs

Coordinator terminal 1:

```text
python server/demo.py gfd
```

Expected: `GFD: 0 members`. Leave it running. Tell the group:

> GFD is running. Please start only your LFD in terminal 1 now. Keep terminal 2
> ready for your server; wait for my go-ahead before starting the server.

S1 owner, terminal 1:

```text
python server/demo.py lfd1 --gfd-host GFD_IP --replica-host S1_IP
```

S2 owner, terminal 1:

```text
python server/demo.py lfd2 --gfd-host GFD_IP --replica-host S2_IP
```

S3 owner, terminal 1:

```text
python server/demo.py lfd3 --gfd-host GFD_IP --replica-host S3_IP
```

Expected: GFD registers all three LFDs, and heartbeat requests/replies are visible.
Membership remains zero. Local-server-unavailable messages at the LFDs are
expected until the servers start. Leave the LFD terminals running.

Do not use `--local` for the four-laptop test. Each replica uses port 8001 on its
own laptop; distinct IP addresses keep the endpoints separate.

## 8. Start S1, then S2, then S3

Ask the S1 owner to run this in terminal 2:

```text
python server/demo.py s1
```

Wait until GFD lists S1 as its one member. Then ask the S2 owner to run in terminal 2:

```text
python server/demo.py s2
```

Wait until GFD lists S1 and S2. Then ask the S3 owner to run in terminal 2:

```text
python server/demo.py s3
```

Expected: GFD now lists S1, S2, S3. Each LFD exchanges heartbeats with its replica.
Keep all six teammate terminals running. Start clients only after this succeeds.

## 9. Coordinator: check connectivity and launch clients

In the coordinator's spare terminal, substitute the replica IP addresses and run:

```text
python server/demo.py check --replica-host S1_IP
python server/demo.py check --replica-host S2_IP
python server/demo.py check --replica-host S3_IP
```

Each command must print `GFD reachable` and `Replica reachable`. Check that the
replica identity matches the intended S1, S2, or S3. The GFD defaults to localhost,
which is correct because these checks run on the coordinator.

If a check fails, stop here and resolve the IP, process, or firewall/network
problem. Do not proceed to fault injection with a broken baseline.

Now start one client in each of three coordinator terminals:

| Coordinator terminal | Command |
| --- | --- |
| 2 | `python server/demo.py c1` |
| 3 | `python server/demo.py c2` |
| 4 | `python server/demo.py c3` |

Keep GFD running in terminal 1. Wait around 20 seconds and check **every** client:
membership contains all three replicas, request numbers advance, first replies
are delivered, and duplicate replies are discarded. Replicas show received
requests, state changes, and replies. You now have ten running components.

## 10. Demonstrate two sequential faults

Tell the S1 owner:

> Press Ctrl+C only in your S1 server terminal (terminal 2). Keep LFD1 running.

Wait for the fault report and GFD membership S2/S3. Confirm C1, C2, and C3 each
deliver several new replies. Detection is periodic; allow a few seconds rather
than expecting an instantaneous transition.

Then tell the S2 owner:

> Press Ctrl+C only in your S2 server terminal (terminal 2). Keep LFD2 running.

Expected: membership becomes S3. All three clients continue to receive and
deliver new replies from S3. Once older replies drain, new requests have no
replica duplicates because only one replica remains.

Do not restart the killed servers during this run. This milestone has no replica
state recovery; the implementation excludes a fresh empty-state replica once
traffic has begun.

## 11. Reset and rehearse once more

Stop the three clients first. Then stop the LFDs, remaining replica server, and
finally GFD, using Ctrl+C in each running terminal. Confirm each process returns
to its terminal prompt.

Restart from section 7 in this order:

```text
GFD -> LFD1/LFD2/LFD3 -> S1 -> S2 -> S3 -> connectivity checks -> C1/C2/C3
```

Repeat the two-fault sequence once without debugging interruptions. After the
rehearsal, stop the system so the graded demo can begin from zero membership.
If a client or GFD accidentally restarts, perform this full reset too.

## 12. Presenter preparation and demo day

The presenter should explain, in their own words:

- M1 detected a server failure; M2 keeps serving requests using surviving replicas.
- LFDs monitor colocated replicas. GFD monitors LFDs, maintains membership, and
  notifies clients when membership changes.
- Each client sends the same request to all healthy replicas, delivers the first
  reply, and identifies/discards later replies using the request identity.
- Our GFD additionally assigns a global request order. Replicas follow that order
  so concurrent clients produce consistent results. TCP alone orders messages on
  each connection, not the interleaving of independent clients' connections.
- Retried requests return cached results rather than changing state twice.
- Clients and GFD stay alive during replica fault injection. GFD failure, client
  recovery, checkpointing, passive replication, and replica recovery are outside
  this implementation's M2 demonstration scope.

Review [README.md](README.md) for the design and limitations. Ask a teammate to
quiz the presenter on ordering, duplicate detection, and what happens if GFD dies.

Meet about 30 minutes early on demo day, with more time if the location or network
changes. Bring chargers, recheck IP addresses, prepare readable terminal windows,
and start from zero membership when the evaluator is ready. Confirm the room and
slot in course announcements. Use the same tested versions on all machines.

## 13. Updating an existing demo clone later

Stop the demo first and return to the parent folder containing all three repos.
Run these checks:

```text
git -C server status --short
git -C client status --short
git -C LFD status --short
git -C server branch --show-current
git -C client branch --show-current
git -C LFD branch --show-current
```

If status prints changed files, preserve those changes and coordinate before
updating. If a branch is not `milestone-2`, resolve that before continuing. When
the three working trees are clean and on `milestone-2`, run:

```text
git -C server pull --ff-only
git -C client pull --ff-only
git -C LFD pull --ff-only
python server/demo.py setup
python server/demo.py test
```

On macOS/Linux use `python3` for the last two commands if needed. A failed pull
should be investigated; do not use a force push or discard teammates' changes.
After a code fix, all four machines need the same version and the affected checks
must be repeated. Merging into `master` can follow team review; it is not needed
to run the demo from these branches.

## Common problems

| What you see | What to check |
| --- | --- |
| `git` or Python command not found | Install/configure the missing tool, reopen the terminal, and check its version. |
| Cannot open `server/demo.py` | Run from the parent folder containing all three repos. |
| Missing client/LFD file | Clone all three repositories as sibling folders, with names `server`, `client`, and `LFD`. |
| Expected new files are absent | Check that each repository is on `milestone-2`, not `master`. |
| Local server unavailable before section 8 | Expected: the LFD starts before its replica. |
| GFD connection refused or timed out | Check coordinator IP, running GFD, TCP 9000, and network reachability. |
| Replica check fails | Check replica IP, running server, TCP 8001, and network reachability. |
| Address already in use | Identify and stop the previous demo process in its own terminal; do not kill unrelated processes. |
| Fresh replica refused after restart | Stop all components and perform the clean reset in section 11. |
| Unexpected error | Save the exact command, full error text, and relevant logs for diagnosis. |
