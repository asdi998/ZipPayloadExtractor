#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ZipPayloadExtractor —— 从 Android OTA ZIP 包（payload.bin）中快速提取分区 / 文件的
单文件工具，支持本地路径与远程 HTTP(S) URL。

文件结构（上半部分为函数接口/库 API，下半部分为 CLI，二者互不干扰）：

  1. 常量与异常
  2. ProgressUtils / ProgressStats —— 进度显示与速度统计
  3. DataFetcher    —— 数据源抽象（HTTP Range / 本地 mmap），线程安全
  4. ZipUtils       —— ZIP 结构解析：开头本地头快速定位（主）+ 中央目录（兜底）
  5. PayloadExtractor —— payload.bin 清单解析 + 分区提取引擎（两阶段流水线）
  6. FileExtractor  —— ZIP 内普通文件提取（并行分块下载）
  7. ZipPayloadTool —— 高层工具类（库 API 主入口，全部状态在实例内）
  8. 模块级便捷函数  —— list_partitions / extract_partition / extract_file
  9. CLI            —— 仅当以脚本方式运行时生效

性能优化要点：
  * 定位 payload.bin 不再读文件尾部的中央目录：OTA 包中 payload.bin 的本地文件头
    位于 ZIP 开头几十 KB 内（例如数据偏移 4966、头部在 ~4930），只请求开头 64KB
    顺序扫描本地文件头即可拿到数据偏移与压缩大小，1 次 HTTP 往返；窗口按
    64KB→256KB→1MB→4MB 指数扩容兜底，失败再回退中央目录法。
  * payload_metadata.bin 是虚拟条目（无独立 ZIP 条目），与 payload.bin 同偏移，
    即其前缀（24 字节固定头 + manifest + 元数据签名），工具解析清单时已在读取；
    其索引（META-INF/com/android/metadata 的 ota-property-files，记录
    「文件名:纯数据偏移:大小」）可作为 payload.bin 的免中央目录兜底定位，
    并用于交叉校验清单长度。
  * 分区提取采用「抓取-处理」两阶段流水线：操作按 data_offset 排序并把连续操作合并
    成大块读取（请求数减少、网络顺序化）；下载线程与解压线程通过有界队列解耦；
    远程用每线程独立 Session（并发安全），本地用只读 mmap。
  * ZERO 操作不下载也不写零：输出文件先 truncate 到分区镜像大小，零区自动成为文件洞。
  * 三级重试抗抖 CDN：单次 Range 读取应用层重试（指数退避）→ 组级读取重试 →
    整次提取失败自动重试；单组失败不再导致整个分区前功尽弃。
  * 修复：多 dst_extents 写入、压缩方法/未压缩大小的字段索引、ZIP64 本地头尺寸、
    每块下载重复开关输出文件等原有问题。
"""

import argparse
import bz2
import hashlib
import itertools
import lzma
import math
import mmap
import os
import queue
import signal
import struct
import sys
import threading
import time
import zlib
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zstandard

import update_metadata_pb2 as um

# =====================================================================
# 1. 常量与异常
# =====================================================================

ZIP_HEADERS = {
    'END': b"\x50\x4b\x05\x06",
    'LOCAL': b"\x50\x4b\x03\x04",
    'CENTRAL': b"\x50\x4b\x01\x02",
    'END64': b"\x50\x4b\x06\x06",
    'LOCATOR64': b"\x50\x4b\x06\x07",
}

# 压缩方法 -> (名称, 解压函数)；方法 0 为未压缩
COMPRESSION_METHODS = {
    0: ("未压缩", lambda x: x),
    8: ("DEFLATE", lambda x: zlib.decompress(x, -15)),
    12: ("BZIP2", bz2.decompress),
    14: ("LZMA", lzma.decompress),
    93: ("Zstandard", zstandard.ZstdDecompressor().decompress),
    95: ("XZ", lzma.decompress),
}

HEADER_FIXED_SIZE = 24          # payload.bin 固定头大小（magic4+version8+manifest_size8+sig_size4）
CHUNK_SIZE = 1024 * 1024 * 4    # 普通文件分块下载的块大小（4MB）
MAX_RETRIES = 6                 # 单次 Range 读取的应用层重试次数（CDN 断连场景）
GROUP_READ_TRIES = 3            # 分区提取时一组数据的读取尝试次数（组级重试）
DEFAULT_THREADS = 8
__version__ = "3.0.1"
HEAD_SCAN_STEPS = (64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024)  # 快速定位扩容窗口
HEAD_SCAN_MAX = 4 * 1024 * 1024
GROUP_TARGET = 8 * 1024 * 1024  # 合并读取的目标大小（8MB）
GROUP_CAP = 32 * 1024 * 1024    # 单次读取上限（32MB）

# 中央目录文件头（46 字节，含签名）
_CD_STRUCT = struct.Struct("<4sHHHHHHIIIHHHHHII")
# 本地文件头（30 字节，含签名）
_LH_STRUCT = struct.Struct("<IHHHHHIIIHH")
# 共享的 Zstd 解压器（解压器无状态，可跨线程复用）
_ZSTD = zstandard.ZstdDecompressor()


class FileNotFoundInZipError(ValueError):
    """ZIP 中未找到文件"""
    pass


class PartitionNotFoundError(ValueError):
    """payload 中未找到分区"""
    pass


class DownloadInterrupted(Exception):
    """下载被中断"""
    pass


class SourceNotSupported(Exception):
    """不支持的源类型"""
    pass


# =====================================================================
# 2. 进度显示与速度统计
# =====================================================================

class ProgressUtils:
    """进度显示工具类"""

    @staticmethod
    def format_size(size):
        size = int(size)
        if size < 1024:
            return f"{size} B"
        units = ["KB", "MB", "GB", "TB", "PB"]
        shift = (size.bit_length() - 1) // 10
        shift = min(shift, len(units))
        size = f"{(size / (1 << (shift * 10))):.2f}".replace(".00", "")
        return f"{size} {units[shift - 1]}"

    @staticmethod
    def print_progress(current, total, speed=0, elapsed=0):
        percent = current * 100 / total if total else 0
        speed_str = f"{ProgressUtils.format_size(speed)}/s"
        eta = (total - current) / speed if speed > 0 else 0
        eta_str = f"{eta:.1f}s" if eta else "--"
        print(
            f"\r进度: {ProgressUtils.format_size(current)}/{ProgressUtils.format_size(total)} "
            f"({percent:.1f}%) | 速度: {speed_str} | 用时: {elapsed:.1f}s | ETA: {eta_str}",
            end="", flush=True,
        )

    @staticmethod
    def speed_from_history(history):
        """由 (时间, 字节数) 采样列表估算速度（字节/秒）"""
        if len(history) < 2:
            return 0
        dt = history[-1][0] - history[0][0]
        if dt <= 0:
            return 0
        return int(sum(b for _, b in history) / dt)


class ProgressStats:
    """线程安全的下载进度统计（已下载字节数 + 速度采样）"""

    def __init__(self, total, samples=5):
        self.total = total
        self.downloaded = 0
        self._lock = threading.Lock()
        self._history = deque(maxlen=samples)

    def add(self, n):
        with self._lock:
            self.downloaded += n
            self._history.append((time.time(), n))

    def snapshot(self):
        with self._lock:
            return self.downloaded, list(self._history)


# =====================================================================
# 3. 数据获取 DataFetcher
# =====================================================================

class DataFetcher:
    """线程安全的 Range 读取器。

    远程源：每个线程独立 Session（requests.Session 非线程安全），
            并发请求互不干扰，且自动获得独立的 keep-alive 连接。
    本地源：打开一次并 mmap，所有读取退化为内存切片，零系统调用开销。
    """

    def __init__(self, source, threads=DEFAULT_THREADS, max_retries=MAX_RETRIES,
                 stop_event=None):
        self.source = source
        self.remote = DataFetcher.is_remote(source)
        self.max_retries = max_retries
        self.stop_event = stop_event if stop_event is not None else threading.Event()
        self._thread_local = threading.local()
        self._file = None
        self._mmap = None
        if not self.remote:
            try:
                self._file = open(source, "rb")
                self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            except (OSError, ValueError):
                # 空文件等无法 mmap 的情况，回退为逐次 open+seek+read
                self._mmap = None

    # ---------- 基础 ----------

    @staticmethod
    def is_remote(source):
        return source.startswith("http://") or source.startswith("https://")

    def _session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            retry = Retry(
                total=self.max_retries,
                backoff_factor=0.3,
                status_forcelist=[500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry, pool_maxsize=8)
            session = requests.Session()
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self._thread_local.session = session
        return session

    def read(self, start, end=None):
        """读取 [start, end]（含）之间的字节；end 为 None 时读到末尾。线程安全。"""
        if self.stop_event.is_set():
            raise DownloadInterrupted("操作已被中断")
        if self.remote:
            return self._read_remote(start, end)
        return self._read_local(start, end)

    def _read_local(self, start, end=None):
        if self._mmap is not None:
            return self._mmap[start:] if end is None else self._mmap[start:end + 1]
        with open(self.source, "rb") as f:
            f.seek(start)
            return f.read() if end is None else f.read(end - start + 1)

    def _read_remote(self, start, end=None):
        """带应用层重试的 Range 读取（CDN 中途断连等场景，重试同一幂等范围请求）。"""
        headers = {"Range": f"bytes={start}-{end}" if end is not None else f"bytes={start}-"}
        last_error = None
        for attempt in range(self.max_retries + 1):
            if self.stop_event.is_set():
                raise DownloadInterrupted("操作已被中断")
            try:
                r = self._session().get(self.source, headers=headers, timeout=(5, 60))
                # 安全保护：服务器若忽略 Range 并返回整个大文件，会导致内存爆炸
                if r.status_code == 200 and end is not None:
                    cl = r.headers.get("Content-Length")
                    if cl is not None and int(cl) != (end - start + 1):
                        r.close()
                        raise IOError("服务器未正确响应 Range 请求")
                r.raise_for_status()
                return r.content
            except requests.exceptions.RequestException as e:
                if self.stop_event.is_set():
                    raise DownloadInterrupted("操作已被中断") from e
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(0.3 * (2 ** attempt))
        raise IOError(f"读取远程数据失败 [{start}-{end}]: {last_error}")

    def file_size(self):
        """获取源文件总大小：HEAD 优先，失败用 Range bytes=0-0 的 Content-Range 兜底。"""
        if not self.remote:
            return os.path.getsize(self.source)
        try:
            r = self._session().head(self.source, allow_redirects=True, timeout=(5, 30))
            if r.status_code < 400 and "Content-Length" in r.headers:
                return int(r.headers["Content-Length"])
        except requests.exceptions.RequestException:
            pass
        r = self._session().get(self.source, headers={"Range": "bytes=0-0"}, timeout=(5, 30))
        cr = r.headers.get("Content-Range")
        if cr and "/" in cr:
            total = cr.rsplit("/", 1)[1]
            if total.isdigit():
                return int(total)
        raise ValueError("无法获取远程文件大小")

    def close(self):
        if self._mmap is not None:
            try:
                self._mmap.close()
            except Exception:
                pass
        if self._file is not None:
            try:
                self._file.close()
            except Exception:
                pass


# =====================================================================
# 4. ZIP 结构解析 ZipUtils
# =====================================================================

class ZipUtils:
    """ZIP 结构解析：快速定位（开头本地头扫描）+ 兜底（尾部中央目录）。"""

    @staticmethod
    def parse_zip64_extra(extra_field):
        """解析 ZIP64 扩展字段（ID 0x0001），返回可能含 uncomp_size/compressed_size/local_header_offset 的字典"""
        values = {}
        pos = 0
        while pos <= len(extra_field) - 4:
            header_id, size = struct.unpack("<HH", extra_field[pos:pos + 4])
            if header_id == 0x0001:
                data = extra_field[pos + 4:pos + 4 + size]
                ptr = 0
                if size >= 8:
                    values["uncomp_size"] = struct.unpack("<Q", data[ptr:ptr + 8])[0]
                    ptr += 8
                if size >= 16:
                    values["compressed_size"] = struct.unpack("<Q", data[ptr:ptr + 8])[0]
                    ptr += 8
                if size >= 24:
                    values["local_header_offset"] = struct.unpack("<Q", data[ptr:ptr + 8])[0]
                break
            pos += 4 + size
        return values

    # ---------- 快速路径：扫描 ZIP 开头的本地文件头 ----------

    @staticmethod
    def find_entry_in_head(fetcher, file_size, target,
                           steps=HEAD_SCAN_STEPS, max_total=HEAD_SCAN_MAX):
        """只读 ZIP 开头若干字节，顺序扫描本地文件头定位目标文件。

        命中返回条目 dict（data_offset 已直接可用，无需再校验本地头）；
        遇到数据描述符（flag bit 3，本地头尺寸不可信）、窗口耗尽未命中
        等情况返回 None，由调用方回退中央目录法。
        读取成本：通常 1 次 64KB 请求；窗口按 steps 指数扩容，最多 max_total。
        """
        data = b""
        pos = 0
        for step in steps:
            end = min(len(data) + step, file_size)
            if end > len(data):
                data += fetcher.read(len(data), end - 1)
            while pos + 30 <= len(data):
                if fetcher.stop_event.is_set():
                    return None
                if data[pos:pos + 4] != ZIP_HEADERS['LOCAL']:
                    pos += 1
                    continue
                (_, _, flag, method, _, _, _, csize, ucsize, nlen, elen) = \
                    _LH_STRUCT.unpack_from(data, pos)
                if pos + 30 + nlen > len(data):
                    break  # 文件名被窗口截断 → 扩大窗口后从同一位置继续
                name = data[pos + 30:pos + 30 + nlen].decode("utf-8", "replace")
                if flag & 0x08:
                    return None  # 数据描述符：本地头中的尺寸不可信
                if pos + 30 + nlen + elen > len(data):
                    break  # 扩展字段被窗口截断 → 扩大窗口
                real_csize, real_ucsize = csize, ucsize
                if csize == 0xFFFFFFFF or ucsize == 0xFFFFFFFF:
                    extra = data[pos + 30 + nlen:pos + 30 + nlen + elen]
                    zv = ZipUtils.parse_zip64_extra(extra)
                    real_csize = zv.get("compressed_size", csize)
                    real_ucsize = zv.get("uncomp_size", ucsize)
                    if real_csize == 0xFFFFFFFF or real_ucsize == 0xFFFFFFFF:
                        return None
                if name == target:
                    return {
                        "name": name,
                        "method": method,
                        "flag": flag,
                        "compressed_size": real_csize,
                        "uncompressed_size": real_ucsize,
                        "local_offset": pos,
                        "data_offset": pos + 30 + nlen + elen,
                    }
                # 跳到下一个本地文件头（跳过本文件的数据体，payload.bin 的
                # 8GB 数据因此只消耗一次加法运算，不会逐字节扫描）
                pos += 30 + nlen + elen + real_csize
            if len(data) >= file_size:
                break
        return None

    # ---------- 兜底路径：尾部中央目录 ----------

    @staticmethod
    def find_zip_structure(fetcher, file_size):
        """读取文件尾部找 EOCD / ZIP64 EOCD，返回 (中央目录偏移, 中央目录大小)。"""
        search_end = min(1024 * 1024, file_size)
        end_chunk = fetcher.read(file_size - search_end, file_size - 1)

        locator_pos = end_chunk.rfind(ZIP_HEADERS['LOCATOR64'])
        if locator_pos != -1:
            locator_offset = file_size - search_end + locator_pos
            end_offset = struct.unpack(
                "<Q", fetcher.read(locator_offset + 8, locator_offset + 15)
            )[0]
            tail = min(end_offset + 1023, file_size - 1)
            zip64_end = fetcher.read(end_offset, tail)
            cd_offset = struct.unpack("<Q", zip64_end[48:56])[0]
            cd_size = struct.unpack("<Q", zip64_end[40:48])[0]
            return cd_offset, cd_size

        end_pos = end_chunk.rfind(ZIP_HEADERS['END'])
        if end_pos != -1:
            end_header = end_chunk[end_pos:end_pos + 22]
            cd_offset = struct.unpack("<I", end_header[16:20])[0]
            cd_size = struct.unpack("<I", end_header[12:16])[0]
            return cd_offset, cd_size
        raise ValueError("无法识别ZIP文件结构")

    @staticmethod
    def scan_central_directory(cd_data):
        """解析中央目录数据，返回条目列表（含 ZIP64 尺寸/偏移修正）。"""
        entries = []
        pos, n = 0, len(cd_data)
        while pos + 46 <= n:
            if cd_data[pos:pos + 4] != ZIP_HEADERS['CENTRAL']:
                pos += 1
                continue
            h = _CD_STRUCT.unpack_from(cd_data, pos)
            name_len, extra_len, comment_len = h[10], h[11], h[12]
            name = cd_data[pos + 46:pos + 46 + name_len].decode("utf-8", "replace")
            extra = cd_data[pos + 46 + name_len:pos + 46 + name_len + extra_len]
            zv = ZipUtils.parse_zip64_extra(extra)
            entries.append({
                "name": name,
                "method": h[4],
                "flag": h[3],
                "compressed_size": zv.get("compressed_size", h[8]),
                "uncompressed_size": zv.get("uncomp_size", h[9]),
                "local_offset": zv.get("local_header_offset", h[16]),
                "data_offset": None,
            })
            pos += 46 + name_len + extra_len + comment_len
        return entries

    @staticmethod
    def resolve_data_offset(fetcher, entry):
        """中央目录条目 → 读取并校验本地头，返回数据偏移；失败走启发式搜索。"""
        local_offset = entry["local_offset"]
        try:
            header = fetcher.read(local_offset, local_offset + 29)
            if len(header) < 30 or header[:4] != ZIP_HEADERS['LOCAL']:
                raise ValueError("本地头签名无效")
            nlen = struct.unpack_from("<H", header, 26)[0]
            full = fetcher.read(local_offset, local_offset + 29 + nlen)
            name = full[30:30 + nlen].decode("utf-8", "replace")
            if name != entry["name"]:
                raise ValueError(f"本地头文件名不匹配: {name} vs {entry['name']}")
            elen = struct.unpack_from("<H", header, 28)[0]
            return local_offset + 30 + nlen + elen
        except Exception:
            return ZipUtils.heuristic_search(fetcher, local_offset, entry["name"])

    @staticmethod
    def parse_ota_property_files(metadata_text):
        """解析 META-INF/com/android/metadata 中的 ota-property-files 索引。

        OTA 包的 metadata 文本里，``ota-property-files`` 与
        ``ota-streaming-property-files`` 记录 ``文件名:数据偏移:大小`` 列表。
        注意其中的 payload_metadata.bin 是虚拟条目（无独立 ZIP 条目），
        与 payload.bin 同偏移 —— 它就是 payload.bin 的前缀
        （24 字节固定头 + manifest + 元数据签名）。返回 {文件名: (偏移, 大小)}。
        """
        result = {}
        for line in metadata_text.splitlines():
            line = line.strip()
            if not line.startswith("ota-property-files") or "=" not in line:
                continue
            value = line.split("=", 1)[1].strip().strip('"').strip("'")
            for token in value.split(","):
                parts = token.strip().split(":")
                if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
                    result[parts[0]] = (int(parts[1]), int(parts[2]))
        return result

    @staticmethod
    def heuristic_search(fetcher, base_offset, filename):
        """在给定偏移附近搜索目标文件的本地头（容错）"""
        search_start = max(0, base_offset - 1024)
        search_data = fetcher.read(search_start, base_offset + 1024)
        target_header = ZIP_HEADERS['LOCAL'] + filename.encode()
        found_pos = search_data.find(target_header)
        if found_pos != -1:
            return (
                search_start + found_pos + 30 + len(filename) +
                struct.unpack("<H", search_data[found_pos + 28:found_pos + 30])[0]
            )
        raise ValueError("自动修正失败，请检查ZIP文件完整性")


# =====================================================================
# 5. payload.bin 解析与分区提取 PayloadExtractor
# =====================================================================

class PayloadExtractor:
    """payload.bin（ChromeOS update_engine 格式）清单解析与分区提取引擎。"""

    @staticmethod
    def parse_payload_header(fetcher, payload_offset, file_size):
        """解析 payload.bin 头部，返回 (数据起始偏移, 分区列表, block_size)。"""
        end = min(payload_offset + 512 * 1024 - 1, file_size - 1)
        header = fetcher.read(payload_offset, end)
        if len(header) < HEADER_FIXED_SIZE or header[:4] != b"CrAU":
            raise ValueError("无效的payload.bin格式")

        manifest_size = int.from_bytes(header[12:20], byteorder="big")
        metadata_sig_size = int.from_bytes(header[20:24], byteorder="big")
        partitions_start = HEADER_FIXED_SIZE + manifest_size + metadata_sig_size
        if partitions_start > len(header):
            raise ValueError("头部数据不足，无法解析清单")

        manifest = header[24:24 + manifest_size]
        dam = um.DeltaArchiveManifest()
        dam.ParseFromString(manifest)
        if not dam.partitions:
            raise ValueError("未找到有效分区")
        return partitions_start, dam.partitions, dam.block_size or 4096

    @staticmethod
    def process_operation(op, data, block_size):
        """校验并解压单个操作的数据"""
        if op.data_sha256_hash:
            actual_hash = hashlib.sha256(data).digest()
            if actual_hash != op.data_sha256_hash:
                raise ValueError(f"操作 {op.data_offset} 哈希校验失败")

        if op.type == op.ZERO:
            blocks = sum(e.num_blocks for e in op.dst_extents)
            return b"\x00" * (blocks * block_size)

        decompressors = {
            op.REPLACE_XZ: lzma.decompress,
            op.ZSTD: _ZSTD.decompress,
            op.REPLACE_BZ: bz2.decompress,
            op.REPLACE: lambda x: x,
        }
        decompressor = decompressors.get(op.type)
        if decompressor is None:
            raise ValueError(f"不支持的操作类型: {op.type}")
        return decompressor(data)

    # ---------- 分区提取（两阶段流水线） ----------

    @staticmethod
    def _image_size(partition, block_size):
        """分区镜像大小：优先取清单记录值，否则按目标 extent 推算。"""
        size = partition.new_partition_info.size
        if size and size > 0:
            return size
        max_end = 0
        for op in partition.operations:
            for ext in op.dst_extents:
                max_end = max(max_end, (ext.start_block + ext.num_blocks) * block_size)
        return max_end

    @staticmethod
    def _build_groups(ops, target=GROUP_TARGET, cap=GROUP_CAP):
        """把按 data_offset 排序的操作合并成连续读组。

        组内各操作的压缩数据在源文件中物理连续，合并后一次 Range 请求
        拉整块，显著减少请求数与 RTT 开销；组大小控制在 [target, cap]。
        返回 [(组起始偏移, 组长度, [(op, 组内相对偏移, 长度), ...]), ...]
        """
        groups = []
        items = []
        gstart, glen = 0, 0
        for op in ops:
            start, length = op.data_offset, op.data_length
            if items and start == gstart + glen and glen + length <= cap:
                items.append((op, glen, length))
                glen += length
            else:
                if items:
                    groups.append((gstart, glen, items))
                items = [(op, 0, length)]
                gstart, glen = start, length
            if glen >= target:
                groups.append((gstart, glen, items))
                items, gstart, glen = [], 0, 0
        if items:
            groups.append((gstart, glen, items))
        return groups

    @staticmethod
    def _write_operation(f, op, data, block_size, lock, flush=False):
        """按目标 extent 顺序把解压数据写入输出文件（支持多 extent）。

        返回 (起始字节, 结束字节)，供上层跟踪已写区域；无 extent 时返回 None。
        flush=True 时在写锁内立即落盘（流式模式需要让其他读句柄立即可见）。
        """
        if not op.dst_extents:
            return None
        pos = 0
        start = None
        end = 0
        with lock:
            for ext in op.dst_extents:
                n = ext.num_blocks * block_size
                off = ext.start_block * block_size
                f.seek(off)
                f.write(data[pos:pos + n])
                if start is None:
                    start = off
                end = off + n
                pos += n
            if flush:
                f.flush()
        if pos != len(data):
            raise ValueError("解压数据大小与目标范围不匹配")
        return start, end

    @staticmethod
    def extract_partition(fetcher, partition, output_path, base_offset, block_size,
                          threads, on_progress=None, on_written=None):
        """提取分区镜像。

        base_offset 为 payload.bin 数据区起始（payload_offset + partitions_start）。
        ZERO 操作直接跳过（输出文件已按镜像大小 truncate，零区自动成洞）；
        其余操作按 data_offset 排序合并分组：抓取线程拉数据入有界队列，
        处理线程解压并写入，网络与 CPU 并行且互不阻塞。

        on_progress(current, total, speed, elapsed)：下载字节进度。
        on_written(prefix_bytes)：已连续写满的前缀字节数（从文件头算起，
        ZERO 区域因 truncate 已确定为零，会提前计入），可用于流式推送。
        """
        stop = fetcher.stop_event
        ops = [op for op in partition.operations
               if op.type != op.ZERO and op.data_length > 0]
        image_size = PayloadExtractor._image_size(partition, block_size)
        total = sum(op.data_length for op in ops)
        stats = ProgressStats(total)

        # 预登记 ZERO 区域：truncate 后这些区域内容确定为零，计入已写前缀
        zero_regions = []
        for op in partition.operations:
            if op.type == op.ZERO:
                for ext in op.dst_extents:
                    zero_regions.append((ext.start_block * block_size,
                                         (ext.start_block + ext.num_blocks) * block_size))

        # 已写区域跟踪：合并区间并计算从文件头开始的连续前缀
        region_lock = threading.Lock()
        regions = []
        prefix = 0

        def register_written(start, end):
            nonlocal prefix
            if start is None:
                return
            with region_lock:
                regions.append([start, end])
                regions.sort(key=lambda r: r[0])
                merged = []
                for r in regions:
                    if merged and r[0] <= merged[-1][1]:
                        merged[-1][1] = max(merged[-1][1], r[1])
                    else:
                        merged.append(r)
                regions[:] = merged
                p = 0
                for s, e in regions:
                    if s > p:
                        break
                    p = max(p, e)
                changed = True
                while changed:  # 连续前缀继续延伸进 ZERO 区域
                    changed = False
                    for s, e in zero_regions:
                        if s <= p < e:
                            p = e
                            changed = True
                if p > prefix:
                    prefix = p
                    if on_written:
                        try:
                            on_written(p)
                        except Exception:
                            pass

        with open(output_path, "w+b") as out_file:
            out_file.truncate(image_size)
            if not ops:
                return True

            ops.sort(key=lambda op: op.data_offset)
            groups = PayloadExtractor._build_groups(ops)
            work = queue.Queue(maxsize=max(2, threads * 2))
            gindex = itertools.count()
            gindex_lock = threading.Lock()
            write_lock = threading.Lock()
            n_fetchers = max(1, threads)
            n_processors = max(1, threads)

            def fetcher_worker():
                while True:
                    with gindex_lock:
                        i = next(gindex)
                    if i >= len(groups):
                        return
                    if stop.is_set():
                        return
                    gstart, glen, items = groups[i]
                    data = None
                    for attempt in range(GROUP_READ_TRIES):
                        try:
                            data = fetcher.read(base_offset + gstart,
                                                base_offset + gstart + glen - 1)
                            break
                        except DownloadInterrupted:
                            return
                        except Exception as e:
                            if attempt >= GROUP_READ_TRIES - 1:
                                print(f"读取数据失败: {e}")
                                stop.set()
                                return
                            print(f"读取数据失败，重试 {attempt + 1}/{GROUP_READ_TRIES - 1}: {e}")
                            time.sleep(1.5 * (2 ** attempt))
                    stats.add(glen)
                    for op, rel, length in items:
                        work.put((op, data[rel:rel + length]))

            def processor_worker():
                while True:
                    item = work.get()
                    if item is None:
                        work.task_done()
                        return
                    op, data = item
                    try:
                        processed = PayloadExtractor.process_operation(op, data, block_size)
                        written = PayloadExtractor._write_operation(
                            out_file, op, processed, block_size, write_lock,
                            flush=on_written is not None,
                        )
                        if written:
                            register_written(*written)
                    except Exception as e:
                        print(f"操作失败: {e}")
                        stop.set()
                    finally:
                        work.task_done()

            fetchers = [threading.Thread(target=fetcher_worker, daemon=True)
                        for _ in range(n_fetchers)]
            processors = [threading.Thread(target=processor_worker, daemon=True)
                          for _ in range(n_processors)]
            start_time = time.time()
            for t in fetchers + processors:
                t.start()
            try:
                # 阶段一：等待抓取线程结束（所有数据项都已入队或已放弃）
                while any(t.is_alive() for t in fetchers):
                    if on_progress:
                        current, history = stats.snapshot()
                        try:
                            on_progress(current, total,
                                        ProgressUtils.speed_from_history(history),
                                        time.time() - start_time)
                        except Exception:
                            pass
                    time.sleep(0.2)
                # 阶段二：每个处理线程一个哨兵，全部处理完即退出
                for _ in processors:
                    work.put(None)
                while any(t.is_alive() for t in processors):
                    if on_progress:
                        current, history = stats.snapshot()
                        try:
                            on_progress(current, total,
                                        ProgressUtils.speed_from_history(history),
                                        time.time() - start_time)
                        except Exception:
                            pass
                    time.sleep(0.2)
            finally:
                for t in fetchers + processors:
                    t.join(timeout=5)

        if on_progress:
            try:
                on_progress(total, total, 0, time.time() - start_time)
            except Exception:
                pass
        if on_written and not stop.is_set():
            try:
                on_written(image_size)
            except Exception:
                pass
        return not stop.is_set()


# =====================================================================
# 6. 普通文件提取 FileExtractor
# =====================================================================

class FileExtractor:
    """ZIP 内普通文件的提取（未压缩 → 并行直写；压缩 → 并行下载后整体解压）。"""

    @staticmethod
    def extract(fetcher, entry, output_path, threads=DEFAULT_THREADS, on_progress=None):
        if entry["method"] == 0:
            return FileExtractor._extract_stored(fetcher, entry, output_path, threads,
                                                 on_progress)
        return FileExtractor._extract_compressed(fetcher, entry, output_path, threads,
                                                 on_progress)

    @staticmethod
    def _extract_stored(fetcher, entry, output_path, threads, on_progress):
        """未压缩文件：按 4MB 块并行下载，各块独立写回（无顺序依赖）。"""
        stop = fetcher.stop_event
        file_offset = entry["data_offset"]
        file_size = entry["uncompressed_size"]
        chunk_size = min(CHUNK_SIZE, file_size) or 1
        num_chunks = max(1, math.ceil(file_size / CHUNK_SIZE))
        stats = ProgressStats(file_size)
        write_lock = threading.Lock()
        start_time = time.time()

        def fetch(i):
            if stop.is_set():
                return None
            start = i * chunk_size
            end = min(start + chunk_size, file_size)
            if start >= file_size:
                return None
            return i, fetcher.read(file_offset + start, file_offset + end - 1)

        with open(output_path, "w+b") as f:
            f.truncate(file_size)
            with ThreadPoolExecutor(max_workers=threads,
                                    thread_name_prefix="FileFetch") as executor:
                futures = [executor.submit(fetch, i) for i in range(num_chunks)]
                for future in as_completed(futures):
                    if stop.is_set():
                        break
                    result = future.result()
                    if result is None:
                        continue
                    i, data = result
                    with write_lock:
                        f.seek(i * chunk_size)
                        f.write(data)
                    stats.add(len(data))
                    if on_progress:
                        current, history = stats.snapshot()
                        try:
                            on_progress(current, file_size,
                                        ProgressUtils.speed_from_history(history),
                                        time.time() - start_time)
                        except Exception:
                            pass

        if stop.is_set():
            return False
        if on_progress:
            try:
                on_progress(file_size, file_size, 0, time.time() - start_time)
            except Exception:
                pass
        return True

    @staticmethod
    def _extract_compressed(fetcher, entry, output_path, threads, on_progress):
        """压缩文件：并行下载压缩数据（顺序组装）→ 单次解压 → 写入。"""
        stop = fetcher.stop_event
        file_offset = entry["data_offset"]
        compressed_size = entry["compressed_size"]
        uncompressed_size = entry["uncompressed_size"]
        method = entry["method"]
        info = COMPRESSION_METHODS.get(method)
        if info is None:
            raise ValueError(f"不支持的压缩方法: {method}")
        decompressor = info[1]

        chunk_size = min(CHUNK_SIZE, compressed_size) or 1
        num_chunks = max(1, math.ceil(compressed_size / CHUNK_SIZE))
        results = {}
        stats = ProgressStats(compressed_size)
        start_time = time.time()

        def fetch(i):
            if stop.is_set():
                return None
            start = i * chunk_size
            end = min(start + chunk_size, compressed_size)
            if start >= compressed_size:
                return None
            return i, fetcher.read(file_offset + start, file_offset + end - 1)

        with ThreadPoolExecutor(max_workers=threads,
                                thread_name_prefix="FileFetch") as executor:
            futures = [executor.submit(fetch, i) for i in range(num_chunks)]
            for future in as_completed(futures):
                if stop.is_set():
                    break
                result = future.result()
                if result is None:
                    continue
                i, data = result
                results[i] = data
                stats.add(len(data))
                if on_progress:
                    current, history = stats.snapshot()
                    try:
                        on_progress(current, compressed_size,
                                    ProgressUtils.speed_from_history(history),
                                    time.time() - start_time)
                    except Exception:
                        pass

        if stop.is_set():
            return False

        compressed = b"".join(results[i] for i in range(num_chunks))
        data = decompressor(compressed)
        if len(data) != uncompressed_size:
            raise ValueError(f"解压后大小不匹配: 预期 {uncompressed_size}, 实际 {len(data)}")
        with open(output_path, "wb") as f:
            f.write(data)

        if on_progress:
            try:
                on_progress(uncompressed_size, uncompressed_size, 0,
                            time.time() - start_time)
            except Exception:
                pass
        return True


# =====================================================================
# 7. 高层工具类 ZipPayloadTool（库 API 主入口）
# =====================================================================

class ZipPayloadTool:
    """打开一个 ZIP 源（URL 或本地路径）并执行分区/文件操作。

    用法::

        with ZipPayloadTool("update.zip", threads=8) as tool:
            parts = tool.list_partitions()
            tool.extract_partition("boot", "boot.img")
            tool.extract_file("hello.txt", "hello.txt")

    所有状态（Session、缓存、中断标志）都在实例内，可安全重复调用，
    也可并行创建多个实例处理不同源。中断调用 ``tool.stop()``。
    """

    def __init__(self, source, threads=DEFAULT_THREADS, max_retries=MAX_RETRIES,
                 on_progress=None):
        self.source = source
        self.threads = threads
        self.on_progress = on_progress
        self.stop_event = threading.Event()
        self.fetcher = DataFetcher(source, threads=threads, max_retries=max_retries,
                                   stop_event=self.stop_event)
        self.file_size = None
        self._cd_offset = None
        self._cd_size = None
        self._cd_entries = None
        self._head_entries = {}
        self._payload_offset = None
        self._payload_size = None
        self._partitions_start = None
        self._partitions = None
        self._block_size = None
        self.payload_metadata_size = None  # 来自 ota-property-files 的 payload_metadata.bin 大小
        self.metadata_warning = None       # 定位结果与索引不一致时的警告文本

    # ---------- payload 内部信息（只读） ----------
    # 供需要操作级访问的调用方（如 web 服务的内核版本提取）使用，
    # 调用 list_partitions() 后即可读取。

    @property
    def payload_offset(self):
        return self._payload_offset

    @property
    def payload_size(self):
        return self._payload_size

    @property
    def partitions_start(self):
        return self._partitions_start

    @property
    def partitions(self):
        return self._partitions

    @property
    def block_size(self):
        return self._block_size

    # ---------- 生命周期 ----------

    def load(self):
        """获取源文件总大小（所有操作前自动调用，也可手动调用检查源是否可用）。"""
        if self.file_size is None:
            self.file_size = self.fetcher.file_size()
        return True

    def stop(self):
        self.stop_event.set()

    def close(self):
        self.fetcher.close()

    def __enter__(self):
        self.load()
        return self

    def __exit__(self, *exc_info):
        self.close()

    # ---------- 条目定位 ----------

    def _locate_entry(self, name):
        """定位文件条目：先走开头本地头快速扫描，再走 ota-property-files
        索引（仅 payload.bin），最后回退中央目录。"""
        if name in self._head_entries:
            return self._head_entries[name]
        self.load()
        try:
            entry = ZipUtils.find_entry_in_head(self.fetcher, self.file_size, name)
        except DownloadInterrupted:
            raise
        except Exception:
            entry = None
        if entry is None and name == "payload.bin":
            entry = self._locate_payload_via_metadata()
        if entry is not None:
            self._head_entries[name] = entry
            return entry

        entries = self._load_cd_entries()
        if entries is None:
            return None
        for e in entries:
            if e["name"] == name:
                try:
                    e["data_offset"] = ZipUtils.resolve_data_offset(self.fetcher, e)
                except DownloadInterrupted:
                    raise
                except Exception:
                    continue
                return e
        return None

    def _read_entry_content(self, entry):
        """读取条目完整内容（支持未压缩 / DEFLATE）"""
        data = self.fetcher.read(entry["data_offset"],
                                 entry["data_offset"] + entry["compressed_size"] - 1)
        if entry["method"] == 0:
            return data
        if entry["method"] == 8:
            return zlib.decompress(data, -15)
        raise ValueError(f"不支持的压缩方法: {entry['method']}")

    def _locate_payload_via_metadata(self):
        """兜底：解析 META-INF/com/android/metadata 的 ota-property-files，
        直接取 payload.bin 的纯数据偏移与大小（打包工具预计算，无需 ZIP 结构）。
        同时顺带记录 payload_metadata.bin 大小用于校验。"""
        try:
            meta = self._locate_entry("META-INF/com/android/metadata")
            if meta is None:
                return None
            text = self._read_entry_content(meta).decode("utf-8", "replace")
            props = ZipUtils.parse_ota_property_files(text)
            if "payload.bin" not in props:
                return None
            offset, size = props["payload.bin"]
            if not (0 < offset < self.file_size and offset + size <= self.file_size):
                return None
            meta_size = props.get("payload_metadata.bin", (None, None))[1]
            self.payload_metadata_size = meta_size
            return {
                "name": "payload.bin",
                "method": 0,  # 流式 OTA 的 payload.bin 恒为 ZIP_STORED
                "flag": 0,
                "compressed_size": size,
                "uncompressed_size": size,
                "local_offset": None,
                "data_offset": offset,
            }
        except DownloadInterrupted:
            raise
        except Exception:
            return None

    def _load_cd_entries(self):
        if self._cd_entries is not None:
            return self._cd_entries
        if self._cd_offset is None:
            try:
                self._cd_offset, self._cd_size = ZipUtils.find_zip_structure(
                    self.fetcher, self.file_size
                )
            except Exception:
                return None
        try:
            cd_data = self.fetcher.read(self._cd_offset, self._cd_offset + self._cd_size - 1)
            self._cd_entries = ZipUtils.scan_central_directory(cd_data)
        except DownloadInterrupted:
            raise
        except Exception:
            self._cd_entries = []
        return self._cd_entries

    # ---------- 文件操作 ----------

    def list_files(self):
        """返回 ZIP 内全部文件名列表；中央目录不可用时返回 None。"""
        entries = self._load_cd_entries()
        if entries is None:
            return None
        return [e["name"] for e in entries]

    def extract_file(self, name, output=None):
        """提取 ZIP 内任意文件，成功返回 True。文件不存在抛 FileNotFoundInZipError。"""
        if output is None:
            output = os.path.basename(name.replace("\\", "/"))
        entry = self._locate_entry(name)
        if entry is None:
            raise FileNotFoundInZipError(f"ZIP中未找到 {name}")
        try:
            return FileExtractor.extract(self.fetcher, entry, output,
                                         self.threads, self.on_progress)
        except DownloadInterrupted:
            self._remove_partial(output)
            return False

    def _remove_partial(self, path):
        try:
            os.remove(path)
        except OSError:
            pass

    # ---------- payload 操作 ----------

    def _load_payload_info(self):
        """定位并解析 payload.bin（结果缓存）。成功返回 True。"""
        if self._partitions is not None:
            return True
        self.load()
        entry = self._locate_entry("payload.bin")
        if entry is None:
            return False
        self._payload_offset = entry["data_offset"]
        self._payload_size = entry["uncompressed_size"]
        try:
            self._partitions_start, self._partitions, self._block_size = \
                PayloadExtractor.parse_payload_header(
                    self.fetcher, self._payload_offset, self.file_size
                )
        except Exception:
            return False
        # payload_metadata.bin 大小 = 24 字节固定头 + manifest + 元数据签名，
        # 应与解析出的数据区起始偏移一致（不一致说明定位可能出错）
        if self.payload_metadata_size and self.payload_metadata_size != self._partitions_start:
            self.metadata_warning = (
                f"ota-property-files 记录的 payload_metadata 大小 "
                f"({self.payload_metadata_size}) 与清单解析结果 "
                f"({self._partitions_start}) 不一致"
            )
        return True

    def list_partitions(self):
        """返回分区信息列表 ``[{name, image_size, download_size}, ...]``；
        未找到 payload.bin 或清单解析失败返回 None。
        image_size 与提取产物实际大小一致（清单未记录大小时按 extent 推算）。"""
        if not self._load_payload_info():
            return None
        return [{
            "name": p.partition_name,
            "image_size": PayloadExtractor._image_size(p, self._block_size),
            "download_size": sum(op.data_length for op in p.operations),
        } for p in self._partitions]

    def extract_partition(self, name, output=None, on_written=None):
        """提取指定分区为镜像文件，成功返回 True。
        分区不存在抛 PartitionNotFoundError；未找到 payload.bin 抛
        FileNotFoundInZipError。默认输出 ``name.img``。
        on_written：可选回调 (已写连续前缀字节数)，用于流式场景。"""
        if output is None:
            output = name + ".img"
        if not self._load_payload_info():
            raise FileNotFoundInZipError("ZIP中未找到 payload.bin")
        target = next((p for p in self._partitions if p.partition_name == name), None)
        if target is None:
            available = ", ".join(p.partition_name for p in self._partitions)
            raise PartitionNotFoundError(f"未找到分区 '{name}'，可用分区: {available}")
        try:
            return PayloadExtractor.extract_partition(
                self.fetcher, target, output,
                self._payload_offset + self._partitions_start,
                self._block_size, self.threads, self.on_progress, on_written,
            )
        except DownloadInterrupted:
            self._remove_partial(output)
            return False
        except Exception:
            self._remove_partial(output)
            raise


# =====================================================================
# 8. 模块级便捷函数（函数接口）
# =====================================================================

def list_partitions(source, threads=DEFAULT_THREADS, on_progress=None):
    """列出 OTA 分区信息。

    :param source: ZIP 文件 URL 或本地路径
    :return: ``[{name, image_size, download_size}, ...]``；无 payload.bin 返回 None
    """
    with ZipPayloadTool(source, threads=threads, on_progress=on_progress) as tool:
        return tool.list_partitions()


def extract_partition(source, name, output=None, threads=DEFAULT_THREADS, on_progress=None):
    """提取指定分区。

    :param source: ZIP 文件 URL 或本地路径
    :param name: 分区名称（如 "boot"、"system"）
    :param output: 输出路径，默认 ``name.img``
    :param on_progress: 可选回调 ``(current, total, speed, elapsed)``
    :return: 成功 True；分区不存在抛 PartitionNotFoundError
    """
    with ZipPayloadTool(source, threads=threads, on_progress=on_progress) as tool:
        return tool.extract_partition(name, output)


def extract_file(source, name, output=None, threads=DEFAULT_THREADS, on_progress=None):
    """提取 ZIP 内任意文件。

    :param source: ZIP 文件 URL 或本地路径
    :param name: ZIP 内文件名（如 "payload.bin"、"META-INF/com/android/metadata"）
    :param output: 输出路径，默认取文件名
    :param on_progress: 可选回调 ``(current, total, speed, elapsed)``
    :return: 成功 True；文件不存在抛 FileNotFoundInZipError
    """
    with ZipPayloadTool(source, threads=threads, on_progress=on_progress) as tool:
        return tool.extract_file(name, output)


# =====================================================================
# 9. 命令行 CLI（仅当以脚本方式运行时生效，与库 API 完全隔离）
# =====================================================================

def _build_parser():
    parser = argparse.ArgumentParser(
        prog="ZipPayloadExtractor",
        description="从 Android OTA ZIP（payload.bin）中提取分区或文件，"
                    "支持本地路径与 HTTP(S) URL。",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="示例:\n"
               "  列出分区:\n"
               "    python ZipPayloadExtractor.py <zip-url-or-path>\n"
               "  提取分区:\n"
               "    python ZipPayloadExtractor.py <zip-url-or-path> boot\n"
               "    python ZipPayloadExtractor.py <zip-url-or-path> system -o system.img -t 16\n"
               "  提取文件:\n"
               "    python ZipPayloadExtractor.py <zip-url-or-path> payload.bin\n"
               "    python ZipPayloadExtractor.py <zip-url-or-path> META-INF/com/android/metadata\n"
               "    python ZipPayloadExtractor.py -f <zip-url-or-path> boot",
    )
    parser.add_argument("source", metavar="SOURCE",
                        help="ZIP 文件路径或 URL")
    parser.add_argument("name", metavar="NAME", nargs="?",
                        help="要提取的分区名称或文件名（省略则列出分区）")
    parser.add_argument("-l", "--list", action="store_true",
                        help="列出分区后退出")
    parser.add_argument("-o", "--output", metavar="PATH",
                        help="输出路径（默认：分区为 NAME.img，文件为 NAME）")
    parser.add_argument("-t", "--threads", metavar="N", type=int, default=DEFAULT_THREADS,
                        help=f"下载/解压线程数（默认 {DEFAULT_THREADS}）")
    parser.add_argument("--retries", metavar="N", type=int, default=MAX_RETRIES,
                        help=f"单次数据读取的重试次数（默认 {MAX_RETRIES}）")
    parser.add_argument("-f", "--force-file", action="store_true",
                        help="强制把 NAME 当作 ZIP 条目（文件）而非分区")
    parser.add_argument("-q", "--quiet", action="store_true",
                        help="不显示进度")
    parser.add_argument("--version", action="version",
                        version=f"%(prog)s {__version__}")
    return parser


def _make_progress_cb():
    def callback(current, total, speed, elapsed):
        ProgressUtils.print_progress(current, total, speed, elapsed)
    return callback


EXTRACT_RETRIES = 2  # 整次提取因网络失败时的自动重试次数


def _run_extraction(tool, mode, name, output):
    """执行一次提取；因网络失败（返回 False 且非用户中断）时自动重试。

    异常（文件/分区不存在等）不重试，原样抛出由调用方处理。
    """
    attempts = 0
    while True:
        if tool.stop_event.is_set():
            return False
        if mode == "file":
            ok = tool.extract_file(name, output)
        else:
            ok = tool.extract_partition(name, output)
        if ok or tool.stop_event.is_set():
            return ok
        attempts += 1
        if attempts > EXTRACT_RETRIES:
            return False
        print(f"\n提取失败（网络原因），{attempts * 3} 秒后自动重试 "
              f"{attempts}/{EXTRACT_RETRIES} ...")
        time.sleep(3 * attempts)


def _run_cli(argv=None):
    args = _build_parser().parse_args(argv)

    if not DataFetcher.is_remote(args.source) and not os.path.isfile(args.source):
        print(f"错误: 本地文件不存在 - {args.source}")
        return 1

    tool = ZipPayloadTool(args.source, threads=args.threads, max_retries=args.retries,
                          on_progress=None if args.quiet else _make_progress_cb())

    def signal_handler(sig, frame):
        print("\n接收到中断信号，正在终止操作...")
        tool.stop()
        threading.Timer(15.0, lambda: os._exit(1)).start()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        tool.load()
    except Exception as e:
        print(f"错误: {e}")
        tool.close()
        return 1

    source_kind = "远程" if DataFetcher.is_remote(args.source) else "本地"
    print(f"源: {args.source} ({source_kind}, "
          f"{ProgressUtils.format_size(tool.file_size)})")
    if tool.metadata_warning:
        print(f"警告: {tool.metadata_warning}")

    try:
        if args.list or not args.name:
            partitions = tool.list_partitions()
            if not partitions:
                print("错误: 无法解析分区信息（未找到 payload.bin 或清单损坏）")
                return 1
            print("\n可用分区:")
            print(f"{'分区名称':<20} {'镜像大小':>12} {'下载大小':>14}")
            print("-" * 50)
            for p in partitions:
                print(f"{p['name']:<20} "
                      f"{ProgressUtils.format_size(p['image_size']):>12} "
                      f"{ProgressUtils.format_size(p['download_size']):>14}")
            return 0

        name = args.name
        file_out = args.output or os.path.basename(name.replace("\\", "/"))
        part_out = args.output or name + ".img"
        looks_like_file = "/" in name or "." in name

        if args.force_file:
            ok = _run_extraction(tool, "file", name, file_out)
            print()
            if ok:
                print(f"文件已保存至: {file_out}")
                return 0
            print("错误: 文件提取失败")
            return 1

        if looks_like_file and _run_extraction(tool, "file", name, file_out):
            print()
            print(f"文件已保存至: {file_out}")
            return 0
        if _run_extraction(tool, "partition", name, part_out):
            print()
            print(f"文件已保存至: {part_out}")
            return 0
        if not looks_like_file and _run_extraction(tool, "file", name, file_out):
            print()
            print(f"文件已保存至: {file_out}")
            return 0

        if tool.stop_event.is_set():
            print("操作已终止")
            return 1
        print(f"错误: 无法找到分区或文件 '{name}'")
        return 1
    except FileNotFoundInZipError as e:
        print(f"错误: {e}")
        return 1
    except PartitionNotFoundError as e:
        print(f"错误: {e}")
        return 1
    except DownloadInterrupted:
        print("\n操作被中断")
        return 1
    except KeyboardInterrupt:
        print("\n操作被用户中断")
        tool.stop()
        return 1
    except Exception as e:
        print(f"\n发生错误: {e}")
        return 1
    finally:
        tool.close()


def main():
    sys.exit(_run_cli())


if __name__ == "__main__":
    main()
