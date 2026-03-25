#!/usr/bin/env python3
import argparse
import json
import os
import re
import shlex
import shutil
import sys
import time
from pathlib import Path, PurePosixPath

import paramiko


ROOT = Path(__file__).resolve().parent
REMOTE_PIPELINE_LOCAL = ROOT / "sam3_remote_pipeline.py"
REMOTE_LAUNCHER_LOCAL = ROOT / "run_pipeline.sh"
ENV_KEYS = {
    "host": "SAM3_HOST",
    "port": "SAM3_PORT",
    "username": "SAM3_USERNAME",
    "password": "SAM3_PASSWORD",
    "hf_token": "SAM3_HF_TOKEN",
    "drive_folder_id": "SAM3_DRIVE_FOLDER_ID",
    "drive_folder_url": "SAM3_DRIVE_FOLDER_URL",
    "remote_workspace": "SAM3_REMOTE_WORKSPACE",
    "remote_repo": "SAM3_REMOTE_REPO",
    "remote_miniforge": "SAM3_REMOTE_MINIFORGE",
}


DEFAULTS = {
    "host": "127.0.0.1",
    "port": 10022,
    "username": "notebook",
    "hf_token": "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",
    "remote_workspace": "/kaggle/working/SAM3",
    "remote_repo": "/kaggle/working/sam3",
    "remote_miniforge": "/kaggle/working/miniforge3",
}


class RemoteError(RuntimeError):
    pass


class RemoteRunner:
    def __init__(self, host: str, port: int, username: str, password: str):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            host,
            port=port,
            username=username,
            password=password,
            timeout=20,
            banner_timeout=20,
            auth_timeout=20,
        )

    def close(self):
        self.client.close()

    def bash(self, script: str, timeout: int = 3600, check: bool = True) -> tuple[int, str, str]:
        cmd = "bash -lc " + shlex.quote(script)
        stdin, stdout, stderr = self.client.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        code = stdout.channel.recv_exit_status()
        if check and code != 0:
            raise RemoteError(f"remote command failed ({code})\nSTDOUT:\n{out}\nSTDERR:\n{err}")
        return code, out, err

    def write_text(self, remote_path: str, content: str, executable: bool = False):
        script_lines = [
            "python - <<'PY'",
            "from pathlib import Path",
            f"Path({remote_path!r}).write_text({content!r}, encoding='utf-8')",
        ]
        if executable:
            script_lines.append(f"Path({remote_path!r}).chmod(0o755)")
        script_lines.append("PY")
        self.bash("\n".join(script_lines), timeout=600)


def print_step(message: str):
    print(f"\n==> {message}")


def supports_color() -> bool:
    return sys.stdout.isatty() and os.environ.get("TERM", "").lower() != "dumb"


def colorize(text: str, color: str) -> str:
    if not supports_color():
        return text
    colors = {
        "red": "31",
        "green": "32",
        "yellow": "33",
        "blue": "34",
        "cyan": "36",
        "gray": "90",
        "bold": "1",
    }
    return f"\x1b[{colors[color]}m{text}\x1b[0m"


def status_color(status: str | None) -> str:
    mapping = {
        "completed": "green",
        "busy": "yellow",
        "claimed": "yellow",
        "in_progress": "yellow",
        "failed": "red",
        "idle": "blue",
        "pending": "gray",
    }
    return mapping.get((status or "").lower(), "cyan")


def progress_bar(done: int, total: int, width: int = 28) -> str:
    total = max(total, 1)
    done = max(0, min(done, total))
    filled = int(width * done / total)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def shorten(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


def clear_screen():
    if not sys.stdout.isatty():
        return
    sys.stdout.write("\x1b[2J\x1b[H")
    sys.stdout.flush()


def stream_command(r: RemoteRunner, script: str):
    transport = r.client.get_transport()
    if transport is None:
        raise RemoteError("SSH transport is not available.")
    channel = transport.open_session()
    channel.exec_command("bash -lc " + shlex.quote(script))
    return channel


def parse_env_file(path: Path) -> dict:
    values = {}
    if not path.exists():
        raise FileNotFoundError(f".env file not found: {path}")
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        values[key] = value
    return values


def parse_json_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"config file not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("config file must contain a JSON object")
    return data


def extract_drive_folder_id(folder_url: str) -> str | None:
    match = re.search(r"/folders/([A-Za-z0-9_-]+)", folder_url)
    if match:
        return match.group(1)
    match = re.search(r"[?&]id=([A-Za-z0-9_-]+)", folder_url)
    if match:
        return match.group(1)
    return None


def resolve_config(args) -> dict:
    cfg = dict(DEFAULTS)

    env_file = Path(args.env_file) if args.env_file else ROOT / ".env"
    if env_file.exists():
        env_values = parse_env_file(env_file)
        for key, env_name in ENV_KEYS.items():
            if env_name in env_values and env_values[env_name] != "":
                cfg[key] = env_values[env_name]

    if args.config_file:
        json_values = parse_json_config(Path(args.config_file))
        for key in ENV_KEYS:
            if key in json_values and json_values[key] not in (None, ""):
                cfg[key] = json_values[key]

    for key in ENV_KEYS:
        value = getattr(args, key, None)
        if value not in (None, ""):
            cfg[key] = value

    if cfg.get("drive_folder_url") and not cfg.get("drive_folder_id"):
        derived_folder_id = extract_drive_folder_id(cfg["drive_folder_url"])
        if derived_folder_id:
            cfg["drive_folder_id"] = derived_folder_id

    if "port" in cfg:
        cfg["port"] = int(cfg["port"])

    required_by_command = {
        "verify": ["password"],
        "setup": ["password", "drive_folder_url"],
        "upload-pipeline": ["password"],
        "launch": ["password"],
        "status": ["password"],
        "samtop": ["password"],
        "samlog": ["password"],
        "full": ["password", "drive_folder_url"],
    }
    missing = [key for key in required_by_command.get(args.command, []) if cfg.get(key) in (None, "")]
    if missing:
        raise ValueError(
            "Missing required settings: "
            + ", ".join(missing)
            + ". Provide them via CLI flags, --env-file, or --config-file."
        )
    if cfg.get("drive_folder_url") and not cfg.get("drive_folder_id"):
        raise ValueError("Could not extract Google Drive folder ID from drive_folder_url.")

    return cfg


def verify_local_files():
    missing = [str(p) for p in [REMOTE_PIPELINE_LOCAL, REMOTE_LAUNCHER_LOCAL] if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required local files: {', '.join(missing)}")


def verify_remote(r: RemoteRunner):
    print_step("Verifying remote connection and GPU visibility")
    _, out, _ = r.bash("python --version && pwd && nvidia-smi", timeout=300)
    print(out.strip())


def ensure_megacmd(r: RemoteRunner):
    print_step("Ensuring MEGAcmd is installed and logged in")
    script = """
set -e
if ! command -v mega-whoami >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y >/dev/null
  apt-get install -y ca-certificates wget gpg >/dev/null
  install -d /etc/apt/keyrings
  wget -qO- https://mega.nz/linux/repo/xUbuntu_22.04/Release.key | gpg --dearmor > /etc/apt/keyrings/megasync.gpg
  cat >/etc/apt/sources.list.d/megasync.list <<'EOF'
deb [signed-by=/etc/apt/keyrings/megasync.gpg] https://mega.nz/linux/repo/xUbuntu_22.04/ ./
EOF
  apt-get update -y >/dev/null
  apt-get install -y megacmd >/dev/null
fi
command -v mega-whoami >/dev/null 2>&1
mega-whoami
"""
    code, out, err = r.bash(script, timeout=1800, check=False)
    if code != 0:
        raise RemoteError(
            "Failed to install or run MEGAcmd on the remote notebook.\n"
            f"STDOUT:\n{out}\n"
            f"STDERR:\n{err}"
        )
    if "Not logged in" in err or "Not logged in" in out:
        raise RemoteError("MEGA is not logged in on the remote notebook.")
    message = (out or err).strip()
    print(message or "MEGAcmd is installed and responded successfully.")


def bootstrap_workspace(r: RemoteRunner, cfg: dict):
    print_step("Bootstrapping workspace, repo, and Miniforge")
    script = f"""
set -e
mkdir -p {shlex.quote(cfg['remote_workspace'])}/logs {shlex.quote(cfg['remote_workspace'])}/tmp {shlex.quote(cfg['remote_workspace'])}/results
if [ ! -d {shlex.quote(cfg['remote_repo'])}/.git ]; then
  git clone https://github.com/facebookresearch/sam3.git {shlex.quote(cfg['remote_repo'])}
fi
if [ ! -x {shlex.quote(cfg['remote_miniforge'])}/micromamba ]; then
  cd /tmp
  wget -q -O Miniforge3.sh https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh
  bash Miniforge3.sh -b -p {shlex.quote(cfg['remote_miniforge'])}
fi
"""
    r.bash(script, timeout=7200)


def build_env(r: RemoteRunner, cfg: dict):
    print_step("Creating or updating the remote sam3 environment")
    script = f"""
set -e
export MAMBA_ROOT_PREFIX={shlex.quote(cfg['remote_miniforge'])}
source <({shlex.quote(cfg['remote_miniforge'])}/micromamba shell hook -s bash)
if ! {shlex.quote(cfg['remote_miniforge'])}/micromamba env list | grep -q {shlex.quote(cfg['remote_miniforge'] + '/envs/sam3')}; then
  {shlex.quote(cfg['remote_miniforge'])}/micromamba create -y -n sam3 python=3.12 pip
fi
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(cfg['remote_miniforge'] + '/envs/sam3')} python -m pip install --upgrade pip
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(cfg['remote_miniforge'] + '/envs/sam3')} python -m pip install "setuptools<81" wheel
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(cfg['remote_miniforge'] + '/envs/sam3')} python -m pip install torch==2.7.0 torchvision torchaudio --index-url https://download.pytorch.org/whl/cu126
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(cfg['remote_miniforge'] + '/envs/sam3')} python -m pip install torchcodec
cd {shlex.quote(cfg['remote_repo'])}
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(cfg['remote_miniforge'] + '/envs/sam3')} python -m pip install -e .
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(cfg['remote_miniforge'] + '/envs/sam3')} python -m pip install requests beautifulsoup4 gdown einops "opencv-python-headless<4.12" pycocotools psutil decord
"""
    r.bash(script, timeout=7200)


def patch_repo_for_t4(r: RemoteRunner, cfg: dict):
    print_step("Patching the SAM3 repo for T4-safe autocast")
    tracking_path = str(PurePosixPath(cfg["remote_repo"]) / "sam3/model/sam3_tracking_predictor.py")
    inference_path = str(PurePosixPath(cfg["remote_repo"]) / "sam3/model/sam3_video_inference.py")
    script = f"""
set -e
python - <<'PY'
from pathlib import Path
files = [
    Path({tracking_path!r}),
    Path({inference_path!r}),
]
for path in files:
    text = path.read_text(encoding='utf-8')
    if '_SAM3_AUTOCAST_DTYPE' not in text:
        text = text.replace(
            'import torch\\n',
            "import torch\\n\\n"
            "# T4 and older GPUs do not run SAM3's bfloat16 path reliably.\\n"
            "_SAM3_AUTOCAST_DTYPE = (\\n"
            "    torch.bfloat16\\n"
            "    if torch.cuda.is_available() and torch.cuda.get_device_capability(0)[0] >= 8\\n"
            "    else torch.float16\\n"
            ")\\n",
            1,
        )
    text = text.replace('torch.autocast(device_type=\"cuda\", dtype=torch.bfloat16)', 'torch.autocast(device_type=\"cuda\", dtype=_SAM3_AUTOCAST_DTYPE)')
    text = text.replace('.to(torch.bfloat16)', '.to(_SAM3_AUTOCAST_DTYPE)')
    path.write_text(text, encoding='utf-8')
PY
"""
    r.bash(script, timeout=600)


def patch_repo_for_low_ram(r: RemoteRunner, cfg: dict):
    print_step("Patching the SAM3 repo for stable bounded-memory video loading")
    predictor_path = str(PurePosixPath(cfg["remote_repo"]) / "sam3/model/sam3_video_predictor.py")
    script = f"""
set -e
python - <<'PY'
from pathlib import Path

path = Path({predictor_path!r})
text = path.read_text(encoding='utf-8')
original = text
text = text.replace("async_loading_frames=True", "async_loading_frames=False")
text = text.replace('video_loader_type=\"torchcodec\"', 'video_loader_type=\"cv2\"')
text = text.replace("video_loader_type='torchcodec'", "video_loader_type='cv2'")
if text == original:
    print("No predictor default patch was needed.")
else:
    path.write_text(text, encoding='utf-8')
    print(f"Patched {{path}} for cv2 bounded-memory defaults.")
PY
"""
    r.bash(script, timeout=600)


def cache_checkpoint(r: RemoteRunner, cfg: dict):
    print_step("Authenticating Hugging Face and caching the checkpoint")
    env_prefix = cfg["remote_miniforge"] + "/envs/sam3"
    script = f"""
set -e
export HF_TOKEN={shlex.quote(cfg['hf_token'])}
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(env_prefix)} python - <<'PY'
from huggingface_hub import login
from sam3.model_builder import download_ckpt_from_hf
login(token={cfg['hf_token']!r}, add_to_git_credential=False)
print(download_ckpt_from_hf())
PY
"""
    _, out, _ = r.bash(script, timeout=7200)
    print(out.strip())


def generate_manifest(r: RemoteRunner, cfg: dict):
    print_step("Generating the DAV manifest on the remote notebook")
    env_prefix = cfg["remote_miniforge"] + "/envs/sam3"
    manifest_path = str(PurePosixPath(cfg["remote_workspace"]) / "dav_files_manifest.json")
    script = f"""
set -e
{shlex.quote(cfg['remote_miniforge'])}/micromamba run -p {shlex.quote(env_prefix)} python - <<'PY'
import json
import re
from pathlib import Path
import requests
from bs4 import BeautifulSoup
folder_id = {cfg['drive_folder_id']!r}
folder_url = {cfg['drive_folder_url']!r}
html = requests.get(f"https://drive.google.com/embeddedfolderview?id={{folder_id}}#list", timeout=30).text
soup = BeautifulSoup(html, "html.parser")
files = []
for a in soup.find_all("a", href=True):
    href = a["href"]
    text = a.get_text(" ", strip=True)
    m = re.search(r"/file/d/([A-Za-z0-9_-]+)", href) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", href)
    if not m or not text or not text.lower().endswith(".dav"):
        continue
    files.append({{
        "filename": text,
        "relative_path": text,
        "source_folder_id": folder_id,
        "source_folder_url": folder_url,
        "source_file_id": m.group(1),
        "source_url": href,
        "status": "pending",
    }})
seen = set()
dedup = []
for item in files:
    if item["source_file_id"] in seen:
        continue
    seen.add(item["source_file_id"])
    dedup.append(item)
for i, item in enumerate(dedup):
    item["manifest_index"] = i
out = {{
    "version": 1,
    "source_type": "gdrive_folder",
    "source_ref": folder_id,
    "source_url": folder_url,
    "count": len(dedup),
    "files": dedup,
}}
path = Path({manifest_path!r})
path.write_text(json.dumps(out, indent=2), encoding="utf-8")
print(json.dumps({{"manifest_path": str(path), "count": len(dedup)}}, indent=2))
PY
"""
    _, out, _ = r.bash(script, timeout=1800)
    print(out.strip())


def upload_pipeline(r: RemoteRunner):
    print_step("Uploading the remote pipeline scripts")
    verify_local_files()
    r.bash("mkdir -p /kaggle/working/SAM3", timeout=120)
    r.write_text("/kaggle/working/SAM3/sam3_remote_pipeline.py", REMOTE_PIPELINE_LOCAL.read_text(encoding="utf-8"))
    r.write_text("/kaggle/working/SAM3/run_pipeline.sh", REMOTE_LAUNCHER_LOCAL.read_text(encoding="utf-8"), executable=True)
    r.bash("python -m py_compile /kaggle/working/SAM3/sam3_remote_pipeline.py", timeout=600)


def launch_pipeline(r: RemoteRunner):
    print_step("Launching the 2-worker remote pipeline")
    _, out, err = r.bash(
        """
export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True
export HF_TOKEN="${HF_TOKEN:-}"
/kaggle/working/SAM3/run_pipeline.sh launch
/kaggle/working/SAM3/run_pipeline.sh status
""",
        timeout=1800,
    )
    print((out + ("\n" + err if err else "")).strip())


def show_status(r: RemoteRunner):
    print_step("Fetching remote pipeline status")
    _, out, err = r.bash(
        """
/kaggle/working/SAM3/run_pipeline.sh status
echo "===== NVIDIA-SMI ====="
nvidia-smi
echo "===== SESSION ====="
python - <<'PY'
import json
from pathlib import Path
p = Path('/kaggle/working/SAM3/session.json')
print('exists', p.exists())
if p.exists():
    data = json.loads(p.read_text(encoding='utf-8'))
    print(json.dumps(data.get('summary', {}), indent=2))
    print(json.dumps(data.get('current', {}), indent=2))
PY
""",
        timeout=600,
        check=False,
    )
    print((out + ("\n" + err if err else "")).strip())


def fetch_samtop_snapshot(r: RemoteRunner, cfg: dict) -> dict:
    workspace = PurePosixPath(cfg["remote_workspace"])
    session_path = str(workspace / "session.json")
    logs_dir = str(workspace / "logs")
    script = f"""
python - <<'PY'
import json
import os
import subprocess
from pathlib import Path

workspace = Path({str(workspace)!r})
session_path = Path({session_path!r})
logs_dir = Path({logs_dir!r})

def pid_alive(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False

payload = {{
    "workspace": str(workspace),
    "session_exists": session_path.exists(),
    "session": None,
    "worker_processes": {{}},
    "gpus": [],
}}

for worker_name in ["worker_a", "worker_b"]:
    pid_path = workspace / f"{{worker_name}}.pid"
    pid = None
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
        except Exception:
            pid = None
    payload["worker_processes"][worker_name] = {{
        "pid": pid,
        "alive": pid_alive(pid),
        "log_path": str(logs_dir / f"{{worker_name}}.log"),
    }}

if session_path.exists():
    payload["session"] = json.loads(session_path.read_text(encoding="utf-8"))

try:
    proc = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=index,name,utilization.gpu,memory.used,memory.total",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode == 0:
        for raw in proc.stdout.splitlines():
            parts = [part.strip() for part in raw.split(",")]
            if len(parts) >= 5:
                payload["gpus"].append(
                    {{
                        "index": int(parts[0]),
                        "name": parts[1],
                        "utilization_gpu": parts[2],
                        "memory_used": parts[3],
                        "memory_total": parts[4],
                    }}
                )
    elif proc.stderr.strip():
        payload["gpu_error"] = proc.stderr.strip()
except Exception as exc:
    payload["gpu_error"] = str(exc)

print(json.dumps(payload))
PY
"""
    _, out, _ = r.bash(script, timeout=120, check=False)
    data = json.loads(out.strip() or "{}")
    return data


def prompt_progress(item: dict) -> tuple[int, int, str]:
    prompt_parts = []
    completed = 0
    total = 0
    for prompt_name, prompt_state in item.get("prompts", {}).items():
        chunk_total = prompt_state.get("chunk_count") or 0
        chunk_done = len(prompt_state.get("completed_chunks", []))
        completed += chunk_done
        total += chunk_total
        status = prompt_state.get("status", "pending")
        label = f"{prompt_name}:{status}"
        if chunk_total:
            label += f" {chunk_done}/{chunk_total}"
        prompt_parts.append(label)
    return completed, total, " | ".join(prompt_parts)


def current_prompt_label(item: dict) -> str:
    prompts = item.get("prompts", {})
    for prompt_name, prompt_state in prompts.items():
        if prompt_state.get("status") == "in_progress":
            chunk_done = len(prompt_state.get("completed_chunks", []))
            chunk_total = prompt_state.get("chunk_count") or "?"
            return f"{prompt_name} {chunk_done}/{chunk_total}"
    for prompt_name, prompt_state in prompts.items():
        if prompt_state.get("status") == "failed":
            return f"{prompt_name} failed"
    for prompt_name, prompt_state in prompts.items():
        if prompt_state.get("status") == "completed":
            continue
        return f"{prompt_name} pending"
    return "all prompts complete"


def batch_prompt_progress(item: dict) -> tuple[int, int]:
    prompts = item.get("prompts", {})
    total = len(prompts)
    completed = sum(1 for prompt_state in prompts.values() if prompt_state.get("status") == "completed")
    return completed, total


def preprocessing_summary(item: dict) -> str | None:
    preprocessing = item.get("preprocessing", {})
    status = preprocessing.get("status") or "pending"
    if status == "converting":
        pct = preprocessing.get("conversion_progress_pct") or 0.0
        elapsed = preprocessing.get("conversion_elapsed_seconds") or 0.0
        total = preprocessing.get("conversion_total_seconds")
        if total:
            return f"convert {pct:>5.1f}% ({elapsed:.0f}/{total:.0f}s)"
        return f"convert {pct:>5.1f}% ({elapsed:.0f}s)"
    if status == "segmenting":
        return "segmenting chunks"
    if status == "downloading_dav":
        return "downloading dav"
    if status in {"downloaded_dav", "converted", "ready"}:
        return status.replace("_", " ")
    return None


def render_samtop(snapshot: dict, refresh_seconds: float, started_at: float):
    clear_screen()
    width = shutil.get_terminal_size((120, 30)).columns
    session = snapshot.get("session") or {}
    items = session.get("items", [])
    summary = session.get("summary", {})
    workers = session.get("workers", {})
    worker_processes = snapshot.get("worker_processes", {})
    gpus = snapshot.get("gpus", [])

    completed_items = sum(1 for item in items if item.get("status") == "completed")
    failed_items = sum(1 for item in items if item.get("status") == "failed")
    working_items = sum(1 for item in items if item.get("status") in {"claimed", "processing", "in_progress"})
    pending_items = max(len(items) - completed_items - failed_items - working_items, 0)
    total_prompts = summary.get("total_prompts", 0)
    completed_prompts = summary.get("completed_prompts", 0)

    lines = []
    lines.append(colorize("samtop", "bold") + f"  remote={snapshot.get('workspace', '?')}  refresh={refresh_seconds:.1f}s  quit=q")
    lines.append(
        f"uptime {int(time.time() - started_at)}s  updated {session.get('updated_at', 'n/a')}  "
        f"stop_reason {session.get('stop_reason') or 'running'}"
    )
    lines.append("")

    if not snapshot.get("session_exists"):
        lines.append(colorize("No remote session.json yet.", "yellow"))
        lines.append("Run setup/upload/launch first, then reopen samtop.")
    else:
        lines.append(
            f"videos   {progress_bar(completed_items, max(len(items), 1))}  "
            f"{completed_items}/{len(items)} completed  pending={pending_items}  working={working_items}  failed={failed_items}"
        )
        lines.append(
            f"prompts  {progress_bar(completed_prompts, max(total_prompts, 1))}  "
            f"{completed_prompts}/{total_prompts} completed  uploaded={summary.get('uploaded_results', 0)}"
        )
        lines.append("")
        lines.append(colorize("Workers", "cyan"))
        for worker_name in ["worker_a", "worker_b"]:
            worker = workers.get(worker_name, {})
            proc = worker_processes.get(worker_name, {})
            claimed = worker.get("claimed_task") or {}
            item = None
            manifest_index = claimed.get("manifest_index")
            if manifest_index is not None:
                item = next((entry for entry in items if entry.get("manifest_index") == manifest_index), None)
            status_text = worker.get("status") or ("busy" if proc.get("alive") else "idle")
            line = (
                f"{worker_name:<8} "
                f"{colorize(status_text, status_color(status_text)):<20} "
                f"pid={proc.get('pid') or '-':<7} "
                f"alive={'yes' if proc.get('alive') else 'no ':<3} "
                f"file={shorten(claimed.get('filename', '-'), max(18, width // 4))}"
            )
            lines.append(line)
            if item is not None:
                chunk_done, chunk_total, _ = prompt_progress(item)
                batch_done, batch_total = batch_prompt_progress(item)
                conversion = preprocessing_summary(item)
                lines.append(
                    f"  batch={batch_done}/{batch_total or '?'} prompts  "
                    f"prompt={shorten(current_prompt_label(item), 22)}  "
                    f"chunks={chunk_done}/{chunk_total or '?'}  "
                    f"heartbeat={worker.get('last_heartbeat') or 'n/a'}"
                )
                if conversion:
                    pct = item.get("preprocessing", {}).get("conversion_progress_pct") or 0.0
                    lines.append(
                        f"  prep ={shorten(conversion, 28)}  "
                        f"{progress_bar(int(round(pct)), 100, width=min(20, max(10, width // 8)))}"
                    )

        if gpus:
            lines.append("")
            lines.append(colorize("GPUs", "cyan"))
            for gpu in gpus:
                mem_label = f"{gpu['memory_used']}/{gpu['memory_total']} MiB"
                lines.append(
                    f"gpu{gpu['index']}  {shorten(gpu['name'], 24):<24}  util={gpu['utilization_gpu']:>3}%  mem={mem_label}"
                )

        interesting = []
        for item in items:
            if item.get("status") in {"claimed", "failed", "pending"}:
                interesting.append(item)
        interesting = interesting[: min(8, max(4, shutil.get_terminal_size((120, 30)).lines - 18))]

        if interesting:
            lines.append("")
            lines.append(colorize("Queue", "cyan"))
            for item in interesting:
                chunk_done, chunk_total, detail = prompt_progress(item)
                status_text = item.get("status", "pending")
                batch_done, batch_total = batch_prompt_progress(item)
                prep = preprocessing_summary(item)
                lines.append(
                    f"{item.get('manifest_index', '-'):>3}  "
                    f"{colorize(status_text, status_color(status_text)):<18}  "
                    f"{shorten(item.get('filename', '-'), max(24, width // 3)):<40}  "
                    f"batch={batch_done}/{batch_total or '?'}  chunks={chunk_done}/{chunk_total or '?'}"
                )
                if prep:
                    lines.append(f"     prep: {shorten(prep, width - 12)}")
                if detail:
                    lines.append(f"     {shorten(detail, width - 5)}")

    sys.stdout.write("\n".join(lines[: max(8, shutil.get_terminal_size((120, 30)).lines - 1)]) + "\n")
    sys.stdout.flush()


def wait_for_quit(refresh_seconds: float) -> bool:
    deadline = time.time() + refresh_seconds
    if os.name == "nt":
        import msvcrt

        while time.time() < deadline:
            if msvcrt.kbhit():
                key = msvcrt.getwch()
                if key.lower() == "q":
                    return True
            time.sleep(0.1)
        return False

    import select
    import termios
    import tty

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while time.time() < deadline:
            readable, _, _ = select.select([sys.stdin], [], [], 0.1)
            if readable:
                key = sys.stdin.read(1)
                if key.lower() == "q":
                    return True
        return False
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def run_samtop(r: RemoteRunner, cfg: dict, refresh_seconds: float, once: bool):
    started_at = time.time()
    try:
        while True:
            snapshot = fetch_samtop_snapshot(r, cfg)
            render_samtop(snapshot, refresh_seconds, started_at)
            if once or not sys.stdout.isatty():
                break
            if wait_for_quit(refresh_seconds):
                break
    except KeyboardInterrupt:
        pass
    finally:
        if sys.stdout.isatty():
            print()


def samlog_color(worker_name: str) -> str:
    return {"worker_a": "cyan", "worker_b": "yellow", "stderr": "red"}.get(worker_name, "gray")


def print_samlog_line(worker_name: str | None, line: str):
    text = line.rstrip("\r\n")
    if not text:
        print()
        return
    if worker_name:
        prefix = colorize(f"[{worker_name}]", samlog_color(worker_name))
        print(f"{prefix} {text}")
        return
    print(colorize(text, "gray"))


def run_samlog(r: RemoteRunner, cfg: dict, lines: int):
    workspace = PurePosixPath(cfg["remote_workspace"])
    logs_dir = workspace / "logs"
    script = f"""
set -e
mkdir -p {shlex.quote(str(logs_dir))}
touch {shlex.quote(str(logs_dir / 'worker_a.log'))} {shlex.quote(str(logs_dir / 'worker_b.log'))}
tail -n {max(lines, 1)} -F {shlex.quote(str(logs_dir / 'worker_a.log'))} {shlex.quote(str(logs_dir / 'worker_b.log'))}
"""
    channel = stream_command(r, script)
    channel.settimeout(0.2)
    current_worker = None
    stdout_buffer = ""
    stderr_buffer = ""
    header_map = {
        f"==> {logs_dir / 'worker_a.log'} <==": "worker_a",
        f"==> {logs_dir / 'worker_b.log'} <==": "worker_b",
    }
    if sys.stdout.isatty():
        print(colorize("samlog", "bold") + f"  remote={logs_dir}  ctrl+c to quit")
        print()
    try:
        while True:
            if channel.recv_ready():
                stdout_buffer += channel.recv(4096).decode("utf-8", "replace")
                while "\n" in stdout_buffer:
                    line, stdout_buffer = stdout_buffer.split("\n", 1)
                    stripped = line.strip()
                    if stripped in header_map:
                        current_worker = header_map[stripped]
                        continue
                    print_samlog_line(current_worker, line)
            if channel.recv_stderr_ready():
                stderr_buffer += channel.recv_stderr(4096).decode("utf-8", "replace")
                while "\n" in stderr_buffer:
                    line, stderr_buffer = stderr_buffer.split("\n", 1)
                    print_samlog_line("stderr", line)
            if channel.exit_status_ready():
                while channel.recv_ready():
                    stdout_buffer += channel.recv(4096).decode("utf-8", "replace")
                while channel.recv_stderr_ready():
                    stderr_buffer += channel.recv_stderr(4096).decode("utf-8", "replace")
                break
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass
    finally:
        if stdout_buffer:
            for line in stdout_buffer.splitlines():
                print_samlog_line(current_worker, line)
        if stderr_buffer:
            for line in stderr_buffer.splitlines():
                print_samlog_line("stderr", line)
        channel.close()
        if sys.stdout.isatty():
            print()


def full_setup(r: RemoteRunner, cfg: dict):
    verify_remote(r)
    ensure_megacmd(r)
    bootstrap_workspace(r, cfg)
    build_env(r, cfg)
    patch_repo_for_t4(r, cfg)
    patch_repo_for_low_ram(r, cfg)
    cache_checkpoint(r, cfg)
    generate_manifest(r, cfg)
    upload_pipeline(r)


def parse_args():
    parser = argparse.ArgumentParser(description="Automate the remote SAM3 notebook workflow from the local machine.")
    parser.add_argument("--env-file", help="Optional .env file with SAM3_* variables.")
    parser.add_argument("--config-file", help="Optional JSON config file.")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--hf-token", dest="hf_token")
    parser.add_argument("--drive-folder-id", dest="drive_folder_id")
    parser.add_argument("--drive-folder-url", dest="drive_folder_url")
    parser.add_argument("--remote-workspace", dest="remote_workspace")
    parser.add_argument("--remote-repo", dest="remote_repo")
    parser.add_argument("--remote-miniforge", dest="remote_miniforge")
    parser.add_argument("--refresh-seconds", type=float, default=2.0, help="Refresh interval for samtop.")
    parser.add_argument("--once", action="store_true", help="Render samtop once and exit.")
    parser.add_argument("--lines", type=int, default=80, help="How many existing log lines samlog should show before following.")
    parser.add_argument(
        "command",
        choices=["verify", "setup", "upload-pipeline", "launch", "status", "samtop", "samlog", "full"],
        help="Which automation step to run.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    cfg = resolve_config(args)

    runner = RemoteRunner(cfg["host"], cfg["port"], cfg["username"], cfg["password"])
    try:
        if args.command == "verify":
            verify_remote(runner)
            ensure_megacmd(runner)
        elif args.command == "setup":
            full_setup(runner, cfg)
        elif args.command == "upload-pipeline":
            upload_pipeline(runner)
        elif args.command == "launch":
            launch_pipeline(runner)
        elif args.command == "status":
            show_status(runner)
        elif args.command == "samtop":
            run_samtop(runner, cfg, refresh_seconds=max(args.refresh_seconds, 0.5), once=args.once)
        elif args.command == "samlog":
            run_samlog(runner, cfg, lines=args.lines)
        elif args.command == "full":
            full_setup(runner, cfg)
            launch_pipeline(runner)
            show_status(runner)
    finally:
        runner.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
