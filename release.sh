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

# ---- Write the Python bump script to a temp file (avoids heredoc edge cases) ----
BUMP_PY=".release_bump.py"
cat > "$BUMP_PY" <<'PY'
import sys, re

cfg, new_version = sys.argv[1], sys.argv[2]

def read(p):
    with open(p, 'r', encoding='utf-8') as f:
        return f.read()

def write(p, s):
    with open(p, 'w', encoding='utf-8') as f:
        f.write(s)

def update_pyproject_toml(path, ver):
    s = read(path)
    try:
        import tomlkit
        doc = tomlkit.parse(s)
        project = doc.get('project')
        if project is not None:
            dyn = project.get('dynamic') or []
            if any((str(x).lower() == 'version') for x in dyn):
                print(">> DYNAMIC_VERSION")
                return s
            if 'version' in project:
                project['version'] = tomlkit.string(ver)
                return tomlkit.dumps(doc)
        tool = doc.get('tool') or {}
        poetry = tool.get('poetry')
        if poetry and 'version' in poetry:
            poetry['version'] = tomlkit.string(ver)
            return tomlkit.dumps(doc)
    except Exception:
        pass

    def replace_in_section(src, section_regex, assign_regex, new_line):
        m = re.search(section_regex, src, flags=re.M|re.S)
        if not m:
            return None
        block = m.group(1)
        new_block, n = re.subn(assign_regex, new_line, block, count=1, flags=re.M)
        if n:
            return src[:m.start(1)] + new_block + src[m.end(1):]
        if not block.endswith("\n"):
            block += "\n"
        new_block = block + new_line + "\n"
        return src[:m.start(1)] + new_block + src[m.end(1):]

    res = replace_in_section(s, r'^\[project\]\s*(.*?)(?=^\[|\Z)',
                             r'^\s*version\s*=\s*["\']?[^"\']+["\']?\s*$',
                             f'version = "{ver}"')
    if res is not None:
        return res
    res = replace_in_section(s, r'^\[tool\.poetry\]\s*(.*?)(?=^\[|\Z)',
                             r'^\s*version\s*=\s*["\']?[^"\']+["\']?\s*$',
                             f'version = "{ver}"')
    if res is not None:
        return res
    print(">> NO_VERSION_KEY")
    return s

def update_setup_cfg(path, ver):
    s = read(path)
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
    new_s, _ = re.subn(r'(?s)(setup\([^)]*?\bversion\s*=\s*)(["\'])(.+?)(\2)',
                       r'\1"' + ver + r'"', s, count=1)
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
    print(">> UNSUPPORTED")
PY

# Run the bump and capture stdout markers (e.g., DYNAMIC_VERSION)
PY_OUT="$(python "$BUMP_PY" "$CFG" "$NEW_VERSION")"

# Abort early if dynamic versioning detected
if grep -q ">> DYNAMIC_VERSION" <<<"$PY_OUT"; then
  echo ">> Dynamic version detected in pyproject (version not modified). Aborting release."
  rm -f "$BUMP_PY"
  exit 1
fi

rm -f "$BUMP_PY"

# Ensure clean git state
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if ! git diff --quiet || ! git diff --cached --quiet; then
    echo ">> Working tree has uncommitted changes. Commit or stash before releasing."
    exit 1
  fi
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

# Verify artifact version
echo ">> Verifying artifacts contain version ${NEW_VERSION}"
if ! ls dist/*"${NEW_VERSION}"* >/dev/null 2>&1; then
  echo ">> Built artifacts do not match version ${NEW_VERSION}. Aborting."
  ls -l dist || true
  exit 1
fi

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
  git add pyproject.toml setup.cfg setup.py 2>/dev/null || true
  if git diff --cached --quiet -- pyproject.toml setup.cfg setup.py; then
    echo ">> No version changes staged in version files. Refusing to tag ${TAG}."
    exit 1
  fi
  echo ">> Committing release ${NEW_VERSION}"
  git commit -m "chore(release): ${NEW_VERSION}"
  echo ">> Tagging ${TAG}"
  git tag -a "${TAG}" -m "Release ${NEW_VERSION}"
  echo ">> Pushing commit and tag"
  git push origin HEAD && git push origin "${TAG}"
fi

echo "✅ Done. Released version $NEW_VERSION."
if $USE_TESTPYPI; then
  echo "   (Uploaded to TestPyPI)"
else
  echo "   (Uploaded to PyPI)"
fi
