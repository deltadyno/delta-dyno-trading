#!/bin/bash

echo "================================================"
echo " ADVANCED CRYPTO-MINER & BACKDOOR SCAN (v3)"
echo " Host: $(hostname)"
echo " Kernel: $(uname -r)"
echo " Date: $(date)"
echo "================================================"

# High-confidence miner / backdoor indicators ONLY
PATTERN="xmrig|minerd|monero|c3pool|moneroocean|kdevtmpfsi|syssls|stratum|unmineable|nodebox|cryptonight"

############################################
echo -e "\n[1] Running processes"
ps auxww | sed -n l | grep -Ei "$PATTERN" || echo "✓ None found"

############################################
echo -e "\n[2] systemd services & overrides"
for d in /etc/systemd/system /lib/systemd/system /usr/lib/systemd/system; do
  sudo find "$d" -type f 2>/dev/null | grep -Ei "$PATTERN"
done

echo -e "\n-- Active services"
systemctl list-units --all | grep -Ei "$PATTERN" || echo "✓ None found"

echo -e "\n-- Installed unit files"
systemctl list-unit-files | grep -Ei "$PATTERN" || echo "✓ None found"

echo -e "\n-- systemd timers"
systemctl list-timers --all | grep -Ei "$PATTERN" || echo "✓ None found"

############################################
echo -e "\n[3] Cron & anacron persistence"
sudo grep -REi "$PATTERN" \
  /etc/cron* \
  /var/spool/cron* \
  /etc/anacrontab 2>/dev/null || echo "✓ None found"

############################################
echo -e "\n[4] Startup & environment hooks"
sudo grep -REi "$PATTERN" \
  /etc/profile \
  /etc/profile.d \
  /etc/rc.local \
  /etc/ld.so.preload \
  ~/.bash* \
  /root/.bash* 2>/dev/null || echo "✓ None found"

############################################
echo -e "\n[5] RPM package integrity (binaries only)"
if command -v rpm >/dev/null; then
  sudo rpm -Va | grep -E "^[^c].5|^[^c].U|^[^c].G" || echo "✓ No binary tampering detected"
fi

############################################
echo -e "\n[6] Suspicious executables in PATH"
for d in /usr/bin /usr/sbin /bin /sbin /usr/local/bin; do
  sudo find "$d" -type f -executable -size +1M 2>/dev/null | grep -Ei "$PATTERN"
done || echo "✓ None found"

############################################
echo -e "\n[7] Kernel modules"
lsmod | grep -Ei "$PATTERN" || echo "✓ None loaded"
sudo find /lib/modules/$(uname -r) -type f 2>/dev/null | grep -Ei "$PATTERN" || echo "✓ None found"

############################################
echo -e "\n[8] Immutable files check"
sudo lsattr -R /etc /usr 2>/dev/null | awk '$1 ~ /i/' || echo "✓ No immutable files"

############################################
echo -e "\n[9] SSH persistence"
sudo grep -Ri "command=" /root/.ssh ~/.ssh 2>/dev/null | grep -Ei "$PATTERN" || echo "✓ No forced SSH commands"

############################################
echo -e "\n[10] PAM backdoors"
sudo grep -REi "$PATTERN" /etc/pam.d 2>/dev/null || echo "✓ None found"

############################################
echo -e "\n[11] Hidden mounts"
grep -Ei "overlay|fuse|tmpfs" /proc/self/mounts

############################################
echo -e "\n[12] Network indicators"
sudo ss -tunap | grep -Ei "$PATTERN|3333|4444|5555|7777|14444" || echo "✓ No miner traffic"

############################################
echo -e "\n================================================"
echo " Scan complete."
echo " ✓ If no findings above → system is very likely CLEAN"
echo "================================================"
