"""make doctor: check the workshop toolchain. Prints a tick or cross per tool."""
import importlib
import shutil
import subprocess
import sys

OK, BAD = "✓", "✗"
failed = []


def check(name, ok, detail=""):
    print(f"  {OK if ok else BAD} {name:<22} {detail}")
    if not ok:
        failed.append(name)


def ver(mod):
    try:
        m = importlib.import_module(mod)
        return True, getattr(m, "__version__", "installed")
    except Exception as e:  # noqa: BLE001
        msg = str(e).strip().splitlines()[0][:70] if str(e).strip() else ""
        return False, f"import failed: {e.__class__.__name__} {msg}".strip()


print("Masar workshop doctor\n")
py = sys.version_info
check("python 3.10-3.12", (3, 10) <= (py.major, py.minor) <= (3, 12), f"{py.major}.{py.minor}.{py.micro}")

java = shutil.which("java")
jv = ""
if java:
    r = subprocess.run(["java", "-version"], capture_output=True, text=True)
    jv = (r.stderr or r.stdout).splitlines()[0] if (r.stderr or r.stdout) else ""
check("java 17", bool(java) and ('"17' in jv or "17." in jv), jv or "not found")

for label, mod in [("pyspark", "pyspark"), ("delta-spark", "delta"), ("dbt-core", "dbt.version"),
                   ("great-expectations", "great_expectations"), ("kafka-python", "kafka"),
                   ("pandas", "pandas"), ("pyarrow", "pyarrow")]:
    ok, d = ver(mod)
    check(label, ok, d)

check("git", shutil.which("git") is not None)
docker = shutil.which("docker")
if docker:
    r = subprocess.run(["docker", "info"], capture_output=True, text=True)
    check("docker (Day 2)", r.returncode == 0, "running" if r.returncode == 0 else "installed but not running")
else:
    check("docker (Day 2)", False, "not found")

print()
if failed:
    print(f"{len(failed)} problem(s): {', '.join(failed)}. Fix them before Day 1.")
    sys.exit(1)
print("All good. Next: make data")
