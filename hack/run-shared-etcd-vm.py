#!/usr/bin/env python3
"""Run the scheduled-Pod shared-etcd test in a disposable, offline QEMU guest.

Only explicitly provided binaries, offline images, kernel modules, and test
artifacts enter the guest. No host filesystem, sockets, or external network is
exposed. The guest is discarded when the test ends.
"""
import argparse
import ctypes
import ctypes.util
import gzip
import os
from pathlib import Path
import re
import select
import stat
import subprocess
import tempfile
import time


def module_bytes(path):
    data = path.read_bytes()
    if path.suffix != '.zst':
        return data
    lib = ctypes.CDLL(ctypes.util.find_library('zstd'))
    lib.ZSTD_getFrameContentSize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
    lib.ZSTD_getFrameContentSize.restype = ctypes.c_uint64
    lib.ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
    lib.ZSTD_decompress.restype = ctypes.c_size_t
    size = lib.ZSTD_getFrameContentSize(data, len(data))
    if size > 32 * 1024 * 1024:
        raise ValueError('unexpected module size')
    out = ctypes.create_string_buffer(size)
    result = lib.ZSTD_decompress(out, size, data, len(data))
    if result != size:
        raise ValueError('module decompression failed')
    return out.raw[:result]


def entry(out, name, mode, data=b'', source=None, major=0, minor=0):
    size = source.stat().st_size if source else len(data)
    encoded = name.encode() + b'\0'
    fields = [1, mode, 0, 0, 1, 0, size, 0, 0, major, minor, len(encoded), 0]
    prefix = b'070701' + b''.join(f'{v:08x}'.encode() for v in fields) + encoded
    out.write(prefix + b'\0' * (-len(prefix) % 4))
    if source:
        with source.open('rb') as stream:
            while chunk := stream.read(1024 * 1024):
                out.write(chunk)
    else:
        out.write(data)
    out.write(b'\0' * (-size % 4))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ['qemu-root', 'kernel', 'busybox', 'modules', 'assets', 'runner', 'log']:
        parser.add_argument('--' + arg, type=Path, required=True)
    parser.add_argument('--memory', default='5G')
    parser.add_argument('--timeout', type=int, default=1800)
    args = parser.parse_args()
    names = ['virtio_pci', 'virtio_blk', 'ext4', 'dummy', 'overlay', 'bridge', 'br_netfilter', 'veth', 'vxlan',
             'nf_tables', 'nft_compat', 'nft_chain_nat', 'nft_masq', 'nft_ct',
             'nft_counter', 'nft_fib_inet', 'nft_reject_inet',
             'ip_tables', 'iptable_filter', 'iptable_mangle', 'iptable_raw', 'iptable_nat',
             'ip6_tables', 'ip6table_filter', 'ip6table_mangle', 'ip6table_raw', 'ip6table_nat',
             'nf_conntrack_netlink', 'xt_conntrack', 'xt_tcpudp', 'xt_mark', 'xt_connmark',
             'xt_comment', 'xt_addrtype', 'xt_multiport', 'xt_statistic', 'xt_MASQUERADE',
             'xt_REJECT', 'xt_set', 'ip_set_hash_ip', 'ip_set_hash_net', 'ip_set_hash_ipport']
    modules, visiting = {}, set()
    def add_module(name):
        if name in modules or name in visiting:
            return
        matches = list(args.modules.rglob(name + '.ko*'))
        if not matches:
            return
        visiting.add(name)
        data = module_bytes(matches[0])
        dependencies = re.search(rb'(?:^|\x00)depends=([A-Za-z0-9_,\-]*)\x00', data)
        if dependencies:
            for dep in dependencies[1].decode().split(','):
                if dep:
                    add_module(dep.replace('-', '_'))
        modules[name] = data
    for name in names:
        add_module(name)
    init = '''#!/bin/sh
/bin/busybox --install -s /bin
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
for name in __MODULES__; do insmod /modules/$name.ko || exit 1; done
mkdir -p /newroot
mount -t ext4 /dev/vda /newroot || exit 1
cp -a /bin /binaries /test-runner /var /etc /modules /root /guest-init /newroot/ || exit 1
mkdir -p /newroot/proc /newroot/sys /newroot/dev /newroot/run /newroot/tmp
mount --move /proc /newroot/proc
mount --move /sys /newroot/sys
mount --move /dev /newroot/dev
exec switch_root /newroot /guest-init
'''.replace('__MODULES__', ' '.join(modules))
    guest_init = '''#!/bin/sh
mkdir -p /sys/fs/cgroup /run /var/lib/rancher/k3s/agent/containerd
mount -t cgroup2 cgroup2 /sys/fs/cgroup
mount -t tmpfs tmpfs /run
hostname shared-etcd-lab
ip link set lo up
ip link add eth0 type dummy || exit 1
ip addr add 192.0.2.1/24 dev eth0
ip link set eth0 up
sysctl -w net.ipv4.ip_forward=1
sysctl -w net.bridge.bridge-nf-call-iptables=1
export PATH=/bin:/binaries
export VCLUSTER_SHARED_ETCD_ASSETS=/binaries
export VCLUSTER_SHARED_ETCD_VM=1
export GOMAXPROCS=2
echo SHARED_ETCD_VM_BEGIN
/test-runner -test.v -test.timeout=25m -test.run '^TestScheduledPodsAndCNI$'
result=$?
echo SHARED_ETCD_VM_EXIT=$result
poweroff -f
'''
    modprobe = '''#!/bin/sh
name=$(echo "$1" | tr '-' '_')
[ -d /sys/module/$name ] && exit 0
[ -f /modules/$name.ko ] && exec insmod /modules/$name.ko
exit 1
'''
    files = {
        'bin/busybox': args.busybox, 'binaries/k3s': args.assets / 'k3s',
        'binaries/etcd': args.assets / 'etcd',
        'binaries/kube-apiserver': args.assets / 'kube-apiserver',
        'test-runner': args.runner,
        'var/lib/rancher/k3s/agent/images/system.tar.zst': args.assets / 'k3s-images.tar.zst',
        'var/lib/rancher/k3s/agent/images/workload.tar': args.assets / 'workload-image.tar',
    }
    directories = {'bin', 'binaries', 'dev', 'proc', 'sys', 'run', 'tmp', 'etc', 'modules', 'root'}
    for name in files:
        directories.update(str(p) for p in Path(name).parents if str(p) != '.')
    with tempfile.TemporaryDirectory(prefix='shared-etcd-vm-') as temporary:
        archive = Path(temporary) / 'lab.cpio.gz'
        with gzip.open(archive, 'wb', compresslevel=1) as out:
            for directory in sorted(directories, key=lambda s: (s.count('/'), s)):
                entry(out, directory, stat.S_IFDIR | 0o755)
            for name, source in files.items():
                entry(out, name, stat.S_IFREG | 0o755, source=source)
            entry(out, 'bin/sh', stat.S_IFLNK | 0o777, b'busybox')
            entry(out, 'bin/modprobe', stat.S_IFREG | 0o755, modprobe.encode())
            entry(out, 'etc/passwd', stat.S_IFREG | 0o644, b'root:*:0:0::/root:/bin/sh\n')
            entry(out, 'etc/group', stat.S_IFREG | 0o644, b'root:*:0:\n')
            entry(out, 'etc/resolv.conf', stat.S_IFREG | 0o644, b'nameserver 192.0.2.254\n')
            entry(out, 'dev/console', stat.S_IFCHR | 0o600, major=5, minor=1)
            entry(out, 'dev/null', stat.S_IFCHR | 0o666, major=1, minor=3)
            entry(out, 'init', stat.S_IFREG | 0o755, init.encode())
            entry(out, 'guest-init', stat.S_IFREG | 0o755, guest_init.encode())
            for name, data in modules.items():
                entry(out, 'modules/' + name + '.ko', stat.S_IFREG | 0o644, data)
            entry(out, 'TRAILER!!!', 0)
        env = os.environ.copy()
        env['LD_LIBRARY_PATH'] = str(args.qemu_root / 'usr/lib/x86_64-linux-gnu')
        env['QEMU_MODULE_DIR'] = str(args.qemu_root / 'usr/lib/x86_64-linux-gnu/qemu')
        disk = Path(temporary) / 'rootfs.ext4'
        with disk.open('wb') as stream:
            stream.truncate(4 * 1024**3)
        subprocess.run(['mkfs.ext4', '-q', '-F', str(disk)], check=True)
        command = [str(args.qemu_root / 'usr/bin/qemu-system-x86_64'), '-nodefaults', '-no-user-config',
                   '-no-reboot', '-machine', 'pc,accel=tcg', '-cpu', 'max', '-m', args.memory,
                   '-smp', '2', '-display', 'none', '-monitor', 'none', '-serial', 'stdio', '-net', 'none',
                   '-drive', f'if=none,id=rootfs,file={disk},format=raw',
                   '-device', 'virtio-blk-pci,drive=rootfs',
                   '-L', str(args.qemu_root / 'usr/share/qemu'),
                   '-bios', str(args.qemu_root / 'usr/share/seabios/bios-256k.bin'),
                   '-kernel', str(args.kernel), '-initrd', str(archive),
                   '-append', 'console=ttyS0 rdinit=/init panic=-1 loglevel=3 cgroup_no_v1=all']
        args.log.parent.mkdir(parents=True, exist_ok=True)
        started, result, pending = time.monotonic(), None, b''
        with args.log.open('wb') as log:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
            try:
                while process.poll() is None:
                    if time.monotonic() - started > args.timeout:
                        raise TimeoutError('isolated guest test timed out')
                    ready, _, _ = select.select([process.stdout], [], [], 1)
                    if ready:
                        data = os.read(process.stdout.fileno(), 65536)
                        log.write(data); log.flush()
                        pending += data
                        lines = pending.split(b'\n')
                        pending = lines.pop()
                        for raw in lines:
                            line = raw.decode(errors='replace')
                            if any(tag in line for tag in ['SHARED_ETCD_VM_', '=== RUN', '--- PASS', '--- FAIL', 'scheduled_test.go:']):
                                print(line, flush=True)
                            match = re.search(r'SHARED_ETCD_VM_EXIT=(\d+)', line)
                            if match:
                                result = int(match[1])
                tail = process.stdout.read()
                log.write(tail)
                match = re.search(rb'SHARED_ETCD_VM_EXIT=(\d+)', tail)
                if match:
                    result = int(match[1])
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
        if result != 0:
            raise SystemExit(f'guest test did not pass (result={result}); see {args.log}')
        print('Isolated scheduled-Pod/CNI test passed', flush=True)

if __name__ == '__main__':
    main()
