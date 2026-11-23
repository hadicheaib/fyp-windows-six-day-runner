# Six-day Windows map runner

This Docker Compose project runs two collectors side by side on **one Windows PC**. The west and east collectors each have 5,000 planned map windows in Nabatieh, their own checkpoint volume, and their own Excel export. The routes have no shared camera IDs or modeled viewport overlap. Map windows describe the planned survey; they do not establish coverage of every business.

The two collectors share a durable request ledger and one **144-hour deadline**, measured from the first live start. Time offline counts toward the deadline. The project allows 114 map grants and 114 detail grants per rolling hour across both collectors, with 57 of each per collector. Both preserve unfinished work at the cutoff. A source access stop blocks new grants to both collectors. The request ledger covers this Docker project on this PC; it does not coordinate separate PCs, the Linux VM, or any other collector.

## 1. Set up the Windows PC

- Install and start Docker Desktop with Linux containers. WSL 2 is the recommended backend for this setup.
- Give the Docker engine at least **4 CPU cores** and **10 GiB RAM** for live mode. Offline testing accepts 8 GiB. Plan for a Windows PC with at least 16 GiB RAM and about 20 GB of free disk space. Use a local folder for the project.
- To let the engine come back after reboot, enable Docker Desktop startup at sign-in. The PC must sign in after reboot. Keep it awake while collecting and keep the project's Excel files closed while the exporters write them.
- Confirm PowerShell can see the Linux engine:

  ```powershell
  docker info --format '{{.OSType}} | {{.NCPU}} cores | {{.MemTotal}} bytes'
  docker compose version
  ```

## 2. Get the project and run the offline test

Clone the repository in PowerShell, or select **Code → Download ZIP** on GitHub, extract it, and open PowerShell in the extracted folder.

```powershell
git clone https://github.com/hadicheaib/fyp-windows-six-day-runner.git
cd fyp-windows-six-day-runner
```

Then run:

```powershell
.\run.ps1 -Action Check
.\run.ps1 -Action Start
.\run.ps1 -Action Status
```

The first build can download dependencies. During the test itself, container networking is disabled and the collectors use synthetic data. When the test finishes, `collector-west` and `collector-east` should each show **Exited (0)**. Each processes three synthetic windows and writes six synthetic rows to its workbook:

- `runtime/west/fixture/Fixture_Website_Observations.xlsx`
- `runtime/east/fixture/Fixture_Website_Observations.xlsx`

Allow a few seconds for the exporters to write the files. The fixture volumes and exports are separate from live mode. Running plain `docker compose up --build -d` with the default environment also selects the offline fixture; the PowerShell launcher additionally checks route hashes and engine resources.

## 3. Start, pause, and resume the live run

After the offline test passes, confirm the source access and collection permissions for your use and account for any other active collectors. Use one copy of this assignment. If an earlier PC 5 package already ran, reconcile its checkpoint before using this replacement. A second machine is not a separate source allowance.

```powershell
.\run.ps1 -Mode live -Action Check
.\run.ps1 -Mode live -Action Start
.\run.ps1 -Mode live -Action Status
```

`Start` brings up both collectors and their exporters. The live workbooks are `runtime/west/live/Website_Observations.xlsx` and `runtime/east/live/Website_Observations.xlsx`. Treat these URLs as candidates until their website, entity and Lebanon association are checked. Runtime outputs are for local use and are ignored by Git.

Pause the collectors and let the exporters finish:

```powershell
.\run.ps1 -Mode live -Action Stop
```

Run the same live `Start` command to resume from the saved checkpoints. An ordinary Docker restart preserves state. A manually stopped container stays stopped until started again. The six-day deadline does not reset after a pause, wake, reboot, or repeated `Start`. When the deadline is reached, the collectors stop and the exporters can publish the final checkpoint. Keep all three live volumes: `fyp-pc5-six-day-live_west-state`, `fyp-pc5-six-day-live_east-state`, and `fyp-pc5-six-day-live_host-pace`. Do not run `docker compose down -v` to pause or resume.

## 4. Check progress or troubleshoot

```powershell
.\run.ps1 -Mode live -Action Status
docker compose -p fyp-pc5-six-day-live logs --tail 60 collector-west collector-east exporter-west exporter-east
```

If the collectors are waiting for their hourly slot or for connectivity, leave their volumes alone. If an exporter reports a pending write, close the workbook in Excel and allow it to retry. If the collector reports `needs_attention`, a source stop, a missing shared ledger, or a recovery failure, retain the logs and volumes and inspect the cause before continuing. The setup does not clear stops or replay uncertain source actions automatically. Do not create a second live project against the same routes to restart an expired run.

At 60 finished windows per hour across both collectors, expect about 8,640 in six days. Finishing the complete 10,000-window queue requires about 70 per hour. The estimate depends on listing density, interruptions and time offline.

## Route provenance

The two [pinned route files](routes/manifest.json) come from the project's `LBN-P1-2026-11-19` planning release. They contain planned camera positions and IDs, with no collected listings. Routes use [OpenStreetMap](https://www.openstreetmap.org/copyright) data (© OpenStreetMap contributors, ODbL 1.0) and pinned [geoBoundaries](https://www.geoboundaries.org/) governorate geometry (William & Mary geoLab, CC BY 4.0). The Linux VM assignment is managed separately.
