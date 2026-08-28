# ZipPayloadExtractor

从 Android OTA 升级包（ZIP 中的 `payload.bin`）快速提取分区镜像或文件的工具，
支持本地文件与远程 HTTP(S) 链接，**无需完整下载整个包**。

## 特性

- 🌐 **远程与本地** —— 支持 `https://...` 链接（HTTP Range 请求）与本地文件，可处理数 GB 的大包
- ⚡ **快速定位 payload.bin** —— 只发 1 次 64KB 请求即可定位，不再读取文件尾部的中央目录
- 🚀 **并行提取** —— 「抓取-解压」流水线并行，连续数据合并成大块读取，网络与 CPU 同时跑满
- 📊 **实时进度** —— 流式接收按 128KB 粒度上报，链接限速时进度条依然平滑推进
- 🔁 **抗抖动 CDN** —— 请求级 + 组级 + 整次提取三级重试，中途断连自动续传
- 🕳️ **ZERO 操作零成本** —— 零填充区域不下载也不写盘
- 📄 **任意文件提取** —— `payload.bin`、`META-INF/com/android/metadata` 等 ZIP 条目均可提取
- 📱 **OTA 信息展示** —— 设备代号、Android 版本、安全补丁级别、构建时间（GUI 直接可见）

## 三种使用方式

本工具提供三种使用方式，按需选择：

| 方式 | 适合谁 | 说明 |
| --- | --- | --- |
| 🖥️ **GUI**（图形界面） | 普通用户（Windows） | 输入链接或选择本地文件 → 查看 OTA 信息与分区列表 → 勾选分区一键下载，进度一目了然 |
| ⌨️ **CLI**（命令行） | 脚本/服务器用户 | 一行命令提取指定分区或文件，`python ZipPayloadExtractor.py --help` 查看全部参数 |
| 📦 **函数接口**（Python API） | 开发者 | 把提取能力集成进自己的程序 |

三者共用同一个引擎，功能一致。

## 快速开始

### 普通用户（Windows）

在 **Releases 页面**下载 `ZipPayloadExtractorGUI.exe`（GUI 版，免安装、免 Python），
双击运行：

1. 粘贴 OTA 链接（或点"选择本地文件..."）
2. 点击「获取分区」——自动显示设备/Android 版本/安全补丁等 OTA 信息与全部分区
3. 勾选要下载的分区 → 点「开始下载」，进度条实时显示已下载/总大小

> 下载过的分区再次勾选会自动跳过（已识别同一升级包，换包后自动重新下载）；
> 取消下载不会残留半成品文件。

### 非 Windows 用户 / 开发者（Python 运行）

要求 Python 3.8+：

```bash
pip install -r requirements.txt

# 图形界面
python GUI.pyw

# 命令行：列出分区 / 提取分区 / 提取文件
python ZipPayloadExtractor.py <链接或本地路径>
python ZipPayloadExtractor.py <链接或本地路径> system
python ZipPayloadExtractor.py <链接或本地路径> META-INF/com/android/metadata
```

### 函数接口（嵌入自己的程序）

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
    info = tool.get_ota_info()   # OTA 元数据（设备/版本/安全补丁等）
    tool.stop()                  # 中断进行中的提取
```

## 原理与性能

| 步骤 | 旧实现 | 本工具 |
| --- | --- | --- |
| 定位 payload.bin | 读尾部 1MB 找中央目录，再下载数 MB 目录，再校验本地头 —— 3 次以上往返 | 扫描开头 64KB 本地文件头 —— **1 次往返**（不够时 64KB→4MB 指数扩容，失败回退中央目录） |
| 列分区 | 每次调用重复同样的定位开销 | 每个 `ZipPayloadTool` 实例解析一次并缓存 |
| 提取分区 | 每个操作一次 HTTP 请求、共享非线程安全 Session、下载→解压串行 | 连续操作合并成 8~32MB 大块读取（流式接收）；抓取与解压流水线并行；ZERO 操作完全跳过 |

真实测试（小米 OTA 包，4.95 GB）：列分区仅 **0.37s**，`payload.bin` 位于偏移 4914；
96MB 的 `boot` 分区约 6s 提取完成（普通网络下 15MB/s+）。

## 自行打包 exe

需要打包成 Windows 单文件 exe 时（GUI 版示例）：

```bash
py -3.13 -m nuitka --onefile --assume-yes-for-downloads --output-dir=build-nuitka \
  --include-package-data=certifi --enable-plugin=tk-inter --windows-console-mode=disable \
  --product-name=ZipPayloadExtractor --product-version=3.1.0 \
  --output-filename=ZipPayloadExtractorGUI.exe GUI.pyw
```

> 要求已安装 Nuitka 与 MSVC/zig 工具链；Nuitka 对 Python 3.14 仅为实验性支持，建议用 3.13。

## 文件结构

```
ZipPayloadExtractor.py    # 主程序（引擎 + CLI + 函数接口，单文件）
GUI.pyw                   # 图形界面（tkinter，仅标准库，无额外依赖）
update_metadata_pb2.py    # update_metadata proto 的生成绑定（protobuf 5.27.2，请勿手改）
requirements.txt
```
