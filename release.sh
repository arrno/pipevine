#!/usr/bin/env bash
set -euo pipefail

# release.sh — bump version, build, and upload to PyPI/TestPyPI
# Usage:
#   ./release.sh 0.2.0                # upload to PyPI
#   ./release.sh 0.2.0 --test         # upload to TestPyPI
#   ./release.sh 0.2.0 --tag          # also git tag v0.2.0 and push
#   ./release.sh 0.2.0 --test --tag   # TestPyPI + tag
#
# Requirements: python>=3.8, pip, build, twine, (git optional)
#
# Notes:
# - PyPI will not allow re-uploading an existing version. Always bump.

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <new_version> [--test] [--tag]"
  exit 1
fi

NEW_VERSION="$1"
shift || true

USE_TESTPYPI=false
DO_GIT_TAG=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --test) USE_TESTPYPI=true ;;
    --tag)  DO_GIT_TAG=true ;;
    *) echo "Unknown option: $1" && exit 1 ;;
  esac
  shift
done

# Basic PEP 440-ish sanity check (simple; not exhaustive)
if ! [[ "$NEW_VERSION" =~ ^[0-9]+(\.[0-9]+)*([abrc]\d+)?([.-]post\d+)?([.-]dev\d+)?$ ]]; then
  echo "Warning: '$NEW_VERSION' may not be PEP 440 compliant."
fi

PROJECT_ROOT="$(pwd)"
cd "$PROJECT_ROOT"

# Detect config file
CFG=""
if [[ -f "pyproject.toml" ]]; then
  CFG="pyproject.toml"
elif [[ -f "setup.cfg" ]]; then
  CFG="setup.cfg"
elif [[ -f "setup.py" ]]; then
  CFG="setup.py"
else
  echo "Error: Could not find pyproject.toml, setup.cfg, or setup.py"
  exit 1
fi

echo ">> Detected project config: $CFG"
echo ">> Setting version to: $NEW_VERSION"

# ---- Version bump (robust; preserves TOML when tomlkit is present) ----
python - "$CFG" "$NEW_VERSION" <<'PY'
import sys, re, os, io

cfg, new_version = sys.argv[1], sys.argv[2]

def read(p): 
    with open(p, 'r', encoding='utf-8') as f: 
        return f.read()

def write(p, s): 
    with open(p, 'w', encoding='utf-8') as f: 
        f.write(s)

def update_pyproject_toml(path, ver):
    s = read(path)
    # Try to use tomlkit to preserve formatting/comments
    try:
        import tomlkit
        doc = tomlkit.parse(s)

        # PEP 621
        project = doc.get('project')
        if project is not None:
            dyn = project.get('dynamic') or []
            if any((str(x).lower() == 'version') for x in dyn):
                print(">> pyproject dynamic version detected (not modified)")
                return s
            if 'version' in project:
                project['version'] = tomlkit.string(ver)
                return tomlkit.dumps(doc)

        # Poetry
        tool = doc.get('tool') or {}
        poetry = tool.get('poetry')
        if poetry and 'version' in poetry:
            poetry['version'] = tomlkit.string(ver)
            return tomlkit.dumps(doc)

        # Fallback to regex if keys not found
    except Exception:
        pass

    # Regex fallback scoped to [project] or [tool.poetry]
    def replace_in_section(src, section_regex, assign_regex, new_line):
        m = re.search(section_regex, src, flags=re.M|re.S)
        if not m: 
            return None
        block = m.group(1)
        new_block, n = re.subn(assign_regex, new_line, block, count=1, flags=re.M)
        if n:
            return src[:m.start(1)] + new_block + src[m.end(1):]
        # If no version line, append one to the end of the section block
        if not block.endswith("\n"):
            block += "\n"
        new_block = block + new_line + "\n"
        return src[:m.start(1)] + new_block + src[m.end(1):]

    # [project]
    res = replace_in_section(
        s,
        r'^\[project\]\s*(.*?)(?=^\[|\Z)',
        r'^\s*version\s*=\s*["\']?[^"\']+["\']?\s*$',
        f'version = "{ver}"'
    )
    if res is not None:
        return res

    # [tool.poetry]
    res = replace_in_section(
        s,
        r'^\[tool\.poetry\]\s*(.*?)(?=^\[|\Z)',
        r'^\s*version\s*=\s*["\']?[^"\']+["\']?\s*$',
        f'version = "{ver}"'
    )
    if res is not None:
        return res

    print(">> No 'project.version' or 'tool.poetry.version' found; pyproject left unchanged.")
    return s

def update_setup_cfg(path, ver):
    s = read(path)
    # Operate only inside [metadata] section
    m = re.search(r'^\[metadata\]\s*(.*?)(?=^\[|\Z)', s, flags=re.M|re.S)
    if not m:
        return s
    block = m.group(1)
    new_block, n = re.subn(r'^\s*version\s*=\s*.*$', f'version = {ver}', block, count=1, flags=re.M)
    if not n:
        if not block.endswith("\n"):
            block += "\n"
        new_block = block + f"version = {ver}\n"
    return s[:m.start(1)] + new_block + s[m.end(1):]

def update_setup_py(path, ver):
    s = read(path)
    # Replace setup(... version="x") — fix: no escaping of {ver}
    new_s, n = re.subn(
        r'(?s)(setup\([^)]*?\bversion\s*=\s*)(["\'])(.+?)(\2)',
        r'\1"' + ver + r'"',
        s,
        count=1
    )
    return new_s

if cfg.endswith("pyproject.toml"):
    new_s = update_pyproject_toml(cfg, new_version)
    write(cfg, new_s)
elif cfg.endswith("setup.cfg"):
    new_s = update_setup_cfg(cfg, new_version)
    write(cfg, new_s)
elif cfg.endswith("setup.py"):
    new_s = update_setup_py(cfg, new_version)
    write(cfg, new_s)
else:
    print("Unsupported config file", file=sys.stderr)
    sys.exit(1)
PY

# Show diff (if git repo)
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo ">> Git status after version bump:"
  git add pyproject.toml setup.cfg setup.py 2>/dev/null || true
  git status -s || true
fi

# Clean old build artifacts
echo ">> Cleaning dist/ build/ *.egg-info"
rm -rf dist build ./*.egg-info ./**/.pytest_cache 2>/dev/null || true

# Ensure build + twine are available
python -m pip install --upgrade pip >/dev/null
python -m pip install --upgrade build twine >/dev/null

# Build fresh artifacts
echo ">> Building sdist and wheel"
python -m build

# Choose repository
REPO_URL="https://upload.pypi.org/legacy/"
if $USE_TESTPYPI; then
  REPO_URL="https://test.pypi.org/legacy/"
  echo ">> Using TestPyPI"
fi

# Upload
echo ">> Uploading to: $REPO_URL"
python -m twine upload --repository-url "$REPO_URL" dist/*

# Optional git tag
if $DO_GIT_TAG && git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  TAG="v$NEW_VERSION"
  echo ">> Tagging and pushing: $TAG"
  git commit -m "chore(release): $NEW_VERSION" || true
  git tag -a "$TAG" -m "Release $NEW_VERSION"
  git push --follow-tags || git push && git push --tags
fi

echo "✅ Done. Released version $NEW_VERSION."
if $USE_TESTPYPI; then
  echo "   (Uploaded to TestPyPI)"
else
  echo "   (Uploaded to PyPI)"
fi
