# ZipPayloadExtractor

从 Android OTA ZIP 包（`payload.bin`）中快速提取分区镜像或普通文件的单文件工具，
支持本地路径与远程 HTTP(S) URL，无需完整下载整个包。

整个工具是**单个 Python 文件**：前半部分是干净的库 API（函数接口），
后半部分是命令行 CLI，二者互不干扰。

## 特性

- 🌐 **远程与本地** —— 支持 `https://...` URL（HTTP Range 请求）与本地文件，均可处理数 GB 的大包。
- ⚡ **快速定位 payload.bin** —— 只发 1 次 64KB 请求，扫描 ZIP 开头的本地文件头即可定位
  `payload.bin`，不再读取文件尾部的中央目录（旧方式需要 1MB 尾部 + 数 MB 目录下载）。
- 🚀 **并行分区提取** —— 「抓取-解压」两阶段流水线：连续操作合并成大块读取，
  网络与 CPU 并行，远程使用每线程独立 Session，本地使用 mmap。
- 🕳️ **ZERO 操作零成本** —— 输出文件按镜像大小 truncate，零区自动成为文件洞，不下载也不写零。
- 📄 **文件提取** —— 可提取任意 ZIP 条目（`payload.bin`、`META-INF/com/android/metadata` 等），
  并行分块下载。
- 📦 **ZIP64 支持**、Ctrl+C 中断处理、CDN 断连自动重试。

## 环境要求

- Python 3.8+

```bash
pip install -r requirements.txt
```

## 命令行用法

```
usage: ZipPayloadExtractor.py [-h] [-l] [-o PATH] [-t N] [-f] [-q] [--version]
                              SOURCE [NAME]

positional arguments:
  SOURCE                ZIP 文件路径或 URL
  NAME                  要提取的分区名称或文件名（省略则列出分区）

options:
  -l, --list            列出分区后退出
  -o, --output PATH     输出路径（默认：分区为 NAME.img，文件为 NAME）
  -t, --threads N       下载/解压线程数（默认 8）
  -f, --force-file      强制把 NAME 当作 ZIP 条目（文件）而非分区
  -q, --quiet           不显示进度
```

示例：

```bash
# 列出分区（本地或远程均可）
python ZipPayloadExtractor.py https://example.com/update.zip

# 提取分区
python ZipPayloadExtractor.py https://example.com/update.zip boot
python ZipPayloadExtractor.py https://example.com/update.zip system -o system.img -t 16

# 提取文件
python ZipPayloadExtractor.py https://example.com/update.zip payload.bin
python ZipPayloadExtractor.py https://example.com/update.zip META-INF/com/android/metadata

# 强制按文件提取（例如名称带点号的分区名）
python ZipPayloadExtractor.py -f https://example.com/update.zip boot
```

输出示例：

```
源: update.zip (远程, 4.95 GB)

可用分区:
分区名称               镜像大小      下载大小
--------------------------------------------------
abl                        332 KB       215.28 KB
boot                        96 MB        12.05 MB
system                   869.83 MB      641.72 MB
...
```

## Python 函数接口

```python
import ZipPayloadExtractor as zpe

# 列出分区 -> [{name, image_size, download_size}, ...]；无 payload.bin 返回 None
parts = zpe.list_partitions("update.zip")

# 提取分区（成功返回 True；分区不存在抛 PartitionNotFoundError）
zpe.extract_partition("update.zip", "boot", output="boot.img", threads=8)

# 提取文件（成功返回 True；文件不存在抛 FileNotFoundInZipError）
zpe.extract_file("update.zip", "META-INF/com/android/metadata", output="metadata.txt")

# 高级用法：复用同一个工具实例（解析结果缓存，多次操作零额外请求）
with zpe.ZipPayloadTool("https://example.com/update.zip", threads=16,
                        on_progress=lambda cur, total, speed, elapsed: None) as tool:
    parts = tool.list_partitions()
    tool.extract_partition("system", "system.img")
    tool.extract_file("payload.bin", "payload.bin")

# 中断进行中的提取
tool.stop()
```

## 原理与性能

| 步骤 | 旧实现 | 本工具 |
| --- | --- | --- |
| 定位 payload.bin | 读尾部 1MB 找中央目录，再下载数 MB 目录，再校验本地头 —— 3 次以上往返 | 扫描开头 64KB 本地文件头 —— **1 次往返**（不够时 64KB→4MB 指数扩容，失败回退中央目录） |
| 列分区 | 每次调用重复同样的定位开销 | 每个 `ZipPayloadTool` 实例解析一次并缓存 |
| 提取分区 | 每个操作一次 HTTP 请求、共享非线程安全 Session、下载→解压串行 | 连续操作合并成 8~32MB 大块读取；抓取与解压流水线并行；ZERO 操作完全跳过 |

真实测试（小米 OTA 包，4.95 GB）：列分区仅 **0.37s**，`payload.bin` 位于偏移 4914；
96MB 的 `boot` 分区约 6s 提取完成（普通网络下 15MB/s+）。

## 文件结构

```
ZipPayloadExtractor.py    # 主程序（库 API + CLI，单文件）
update_metadata_pb2.py    # update_metadata proto 的生成绑定（protobuf 5.27.2）
requirements.txt
```

`update_metadata_pb2.py` 由 AOSP 的 `update_metadata.proto`（ChromeOS update engine）
生成，请勿手改。
