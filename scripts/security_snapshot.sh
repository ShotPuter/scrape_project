#!/usr/bin/env bash
# Read-only incident triage: writes a private report, never stops/deletes anything.
# Does not collect process environments, argv, Docker Env, application logs or .env.
set -u
umask 077
snapshot_root=${1:-"data/security-snapshot-$(date -u +%Y%m%dT%H%M%SZ)-$$"}
mkdir -p -- "$snapshot_root" || exit 1
snapshot_root=$(cd -- "$snapshot_root" && pwd)
collect() {
    local name=$1
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout 25 "$@" >"$snapshot_root/$name.txt" 2>&1
    else
        "$@" >"$snapshot_root/$name.txt" 2>&1
    fi
    local result=$?
    printf '%s exit=%s\n' "$name" "$result" >>"$snapshot_root/collection-status.txt"
    return 0
}
collect utc date -u
collect kernel uname -a
collect repository-revision git rev-parse HEAD
collect repository-changes git status --short
collect processes ps -eo pid,ppid,user,comm,pcpu,pmem,etime,lstart --sort=-pcpu
collect network ss -ntup
collect listeners ss -lntup
collect filesystem df -h
collect timers systemctl list-timers --all --no-pager
collect enabled-services systemctl list-unit-files --type=service --state=enabled --no-pager
# List persistence file metadata only; cron commands may contain credentials.
for snapshot_dir in /etc/cron.d /etc/cron.daily /etc/cron.hourly /etc/systemd/system /var/spool/cron; do
    if [ -d "$snapshot_dir" ]; then
        find "$snapshot_dir" -maxdepth 2 -type f -printf '%TY-%Tm-%TdT%TH:%TM:%TS %u %m %p\n' \
            >>"$snapshot_root/persistence-files.txt" 2>>"$snapshot_root/collection-status.txt"
    fi
done
# Link high-CPU process IDs to executable hashes and container cgroups, without
# executing the file or exposing its command-line arguments or environment.
ps -eo pid= --sort=-pcpu | head -n 25 >"$snapshot_root/top-pids.txt"
while read -r snapshot_pid; do
    [[ "$snapshot_pid" =~ ^[0-9]+$ ]] || continue
    snapshot_proc="/proc/$snapshot_pid"
    [ -d "$snapshot_proc" ] || continue
    {
        printf 'PID %s\n' "$snapshot_pid"
        readlink "$snapshot_proc/exe"
        sha256sum "$snapshot_proc/exe"
        cat "$snapshot_proc/cgroup"
        printf '\n'
    } >>"$snapshot_root/process-executables.txt" 2>>"$snapshot_root/collection-status.txt"
done <"$snapshot_root/top-pids.txt"
if command -v docker >/dev/null 2>&1; then
    collect docker-context docker context show
    collect containers docker ps -a --no-trunc --format '{"Id":{{json .ID}},"Name":{{json .Names}},"Image":{{json .Image}},"Status":{{json .Status}},"Ports":{{json .Ports}}}'
    collect container-cpu docker stats --no-stream --format '{{json .}}'
    collect container-security docker ps -q
    head -n 30 "$snapshot_root/container-security.txt" >"$snapshot_root/container-ids.txt"
    while read -r snapshot_container; do
        [[ "$snapshot_container" =~ ^[a-f0-9]{12,64}$ ]] || continue
        collect "container-$snapshot_container-processes" docker top "$snapshot_container" -eo pid,ppid,user,comm,pcpu,pmem,etime
        collect "container-$snapshot_container-security" docker inspect --format \
            '{"Id":{{json .Id}},"Name":{{json .Name}},"ImageId":{{json .Image}},"ImageName":{{json .Config.Image}},"User":{{json .Config.User}},"Pid":{{json .State.Pid}},"Running":{{json .State.Running}},"StartedAt":{{json .State.StartedAt}},"ExitCode":{{json .State.ExitCode}},"Privileged":{{json .HostConfig.Privileged}},"NetworkMode":{{json .HostConfig.NetworkMode}},"PidMode":{{json .HostConfig.PidMode}},"Mounts":{{json .Mounts}}}' \
            "$snapshot_container"
    done <"$snapshot_root/container-ids.txt"
fi
cat >"$snapshot_root/README.txt" <<'NOTE'
Read-only incident snapshot. No processes were stopped and no files deleted.
Process argv/environment, Docker Env, .env and application logs are not collected.
The report still contains internal IP addresses, paths and infrastructure metadata.
Start with processes.txt, network.txt, container-cpu.txt and process-executables.txt.
A high CPU number or an unfamiliar IP alone is not proof of mining.
Correlate the provider's exact alert time, process/container and destination.
An empty network snapshot does not rule out earlier or intermittent connections.
NOTE
printf 'Snapshot saved: %s\n' "$snapshot_root"
printf 'Start with: processes.txt, network.txt, container-cpu.txt, process-executables.txt\n'
