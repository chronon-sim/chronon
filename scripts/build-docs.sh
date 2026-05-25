#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

DOXYGEN_XML_DIR="build/doxygen/xml"
DOXYBOOK_OUTPUT_DIR="website/docs/api"

usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  all       Build everything: doxygen + doxybook2 + docusaurus (default)"
    echo "  doxygen   Run Doxygen only (generate XML)"
    echo "  api       Run Doxygen + doxybook2 (generate API markdown)"
    echo "  site      Build Docusaurus site only (assumes API docs exist)"
    echo "  serve     Start Docusaurus dev server"
    echo "  version   Create a versioned snapshot: $0 version 1.0"
    echo "  clean     Remove all generated docs"
    echo ""
}

check_deps() {
    local missing=()
    command -v doxygen >/dev/null 2>&1 || missing+=("doxygen")
    command -v doxybook2 >/dev/null 2>&1 || missing+=("doxybook2")
    command -v node >/dev/null 2>&1 || missing+=("node")

    if [[ ${#missing[@]} -gt 0 ]]; then
        echo "Missing dependencies: ${missing[*]}"
        echo ""
        echo "Install with:"
        echo "  brew install doxygen"
        echo "  brew install doxybook2  # or: cargo install doxybook2"
        echo "  # Node.js: https://nodejs.org"
        exit 1
    fi
}

run_doxygen() {
    echo "==> Running Doxygen..."
    mkdir -p build/doxygen
    doxygen Doxyfile
    echo "    XML output: $DOXYGEN_XML_DIR"
}

run_doxybook2() {
    echo "==> Running doxybook2..."
    rm -rf "$DOXYBOOK_OUTPUT_DIR"
    mkdir -p "$DOXYBOOK_OUTPUT_DIR"
    doxybook2 \
        --input "$DOXYGEN_XML_DIR" \
        --output "$DOXYBOOK_OUTPUT_DIR" \
        --config website/.doxybook2.json
    echo "    Markdown output: $DOXYBOOK_OUTPUT_DIR"
    fix_frontmatter
}

fix_frontmatter() {
    echo "==> Fixing doxybook2 frontmatter..."
    python3 -c "
import os, re
api_dir = '$DOXYBOOK_OUTPUT_DIR'
fixed = 0
for root, dirs, files in os.walk(api_dir):
    for f in files:
        if not f.endswith('.md'): continue
        path = os.path.join(root, f)
        with open(path) as fh: content = fh.read()
        m = re.match(r'^---\n(.*?)\n---', content, re.DOTALL)
        if not m: continue
        fm = m.group(1)
        new_lines, changed = [], False
        for line in fm.split('\n'):
            kv = re.match(r'^(\w+):\s+(.+)$', line)
            if kv:
                key, val = kv.group(1), kv.group(2)
                if not (val.startswith('\"') and val.endswith('\"')):
                    val_esc = val.replace('\\\\', '\\\\\\\\').replace('\"', '\\\\\"')
                    new_lines.append(f'{key}: \"{val_esc}\"')
                    changed = True
                    continue
            new_lines.append(line)
        if changed:
            new_fm = '\n'.join(new_lines)
            with open(path, 'w') as fh:
                fh.write('---\n' + new_fm + '\n---' + content[m.end():])
            fixed += 1
print(f'    Fixed {fixed} files')
"
}

build_site() {
    echo "==> Building Docusaurus site..."
    cd website
    npm run build
    cd ..
    echo "    Static site: website/build/"
}

serve_site() {
    echo "==> Starting dev server..."
    cd website
    npm start
}

clean() {
    echo "==> Cleaning..."
    rm -rf build/doxygen
    rm -rf "$DOXYBOOK_OUTPUT_DIR"
    rm -rf website/build
    rm -rf website/.docusaurus
    echo "    Done."
}

COMMAND="${1:-all}"

case "$COMMAND" in
    all)
        check_deps
        run_doxygen
        run_doxybook2
        build_site
        echo ""
        echo "Done! Static site at website/build/"
        ;;
    doxygen)
        run_doxygen
        ;;
    api)
        run_doxygen
        run_doxybook2
        ;;
    site)
        build_site
        ;;
    serve)
        serve_site
        ;;
    version)
        VERSION="${2:?Usage: $0 version <version-number>}"
        echo "==> Creating docs version $VERSION..."
        cd website
        npx docusaurus docs:version "$VERSION"
        cd ..
        echo "    Version $VERSION created. Commit the versioned_docs/ and versioned_sidebars/ changes."
        ;;
    clean)
        clean
        ;;
    -h|--help)
        usage
        ;;
    *)
        echo "Unknown command: $COMMAND"
        usage
        exit 1
        ;;
esac
