#!/usr/bin/env bash
#
# claude-cursor-proxy installer
#
# One-liner (macOS / Linux):
#   curl -fsSL https://raw.githubusercontent.com/YeautyYE/claude-cursor-proxy/main/install.sh | bash
#
# Environment variables:
#   GITHUB_REPO                         - Override owner/repo (default: YeautyYE/claude-cursor-proxy)
#   CLAUDE_CURSOR_PROXY_VERSION         - Pin a release tag (e.g. v0.1.22 or 0.1.22)
#   CLAUDE_CURSOR_PROXY_INSTALL_DIR     - Install directory (default: ~/.local/bin, or /usr/local/bin if writable)
#   CLAUDE_CURSOR_PROXY_INSECURE_SKIP_CHECKSUM=1 - Skip checksum verify (not recommended)
#
# Legacy aliases (still accepted): CLAUDE_CURSOR_BRIDGE_*, CLAUDE_CODE_PROXY_*
#
set -euo pipefail

BIN_NAME="claude-cursor-proxy"
REPO="${GITHUB_REPO:-YeautyYE/claude-cursor-proxy}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()    { printf '%b==>%b %s\n' "$BLUE" "$NC" "$1"; }
log_success() { printf '%b==>%b %s\n' "$GREEN" "$NC" "$1"; }
log_warning() { printf '%b==>%b %s\n' "$YELLOW" "$NC" "$1"; }
log_error()   { printf '%bError:%b %s\n' "$RED" "$NC" "$1" >&2; }

need_cmd() {
	if ! command -v "$1" >/dev/null 2>&1; then
		log_error "Required command not found: $1"
		exit 1
	fi
}

download() {
	local url="$1"
	local out="$2"
	if command -v curl >/dev/null 2>&1; then
		curl -fsSL --retry 3 --retry-connrefused --connect-timeout 10 --max-time 120 -o "$out" "$url"
	elif command -v wget >/dev/null 2>&1; then
		wget --tries=3 --timeout=120 -q -O "$out" "$url"
	else
		log_error "Neither curl nor wget found"
		exit 1
	fi
}

detect_platform() {
	local os arch

	case "$(uname -s)" in
	Darwin) os="darwin" ;;
	Linux)  os="linux" ;;
	MINGW*|MSYS*|CYGWIN*)
		log_error "Please use the Windows .zip from GitHub Releases, or install via WSL."
		echo "  https://github.com/${REPO}/releases"
		exit 1
		;;
	*)
		log_error "Unsupported operating system: $(uname -s)"
		echo "${BIN_NAME} ships prebuilt binaries for macOS and Linux."
		echo "Build from source: cargo install --git https://github.com/${REPO}"
		exit 1
		;;
	esac

	case "$(uname -m)" in
	x86_64|amd64)  arch="amd64" ;;
	aarch64|arm64) arch="arm64" ;;
	*)
		log_error "Unsupported architecture: $(uname -m)"
		echo "Prebuilt binaries: amd64 and arm64. Build from source with cargo."
		exit 1
		;;
	esac

	echo "${os}-${arch}"
}

# Normalize to a GitHub release tag (vX.Y.Z) while keeping bare version for assets.
normalize_tag() {
	local raw="$1"
	raw="${raw#refs/tags/}"
	if [[ "$raw" == v* ]]; then
		echo "$raw"
	else
		echo "v${raw}"
	fi
}

pinned_version() {
	# Prefer new env; accept legacy BRIDGE / CODE_PROXY aliases.
	if [ -n "${CLAUDE_CURSOR_PROXY_VERSION:-}" ]; then
		echo "${CLAUDE_CURSOR_PROXY_VERSION}"
	elif [ -n "${CLAUDE_CURSOR_BRIDGE_VERSION:-}" ]; then
		echo "${CLAUDE_CURSOR_BRIDGE_VERSION}"
	elif [ -n "${CLAUDE_CODE_PROXY_VERSION:-}" ]; then
		echo "${CLAUDE_CODE_PROXY_VERSION}"
	else
		echo ""
	fi
}

resolve_version() {
	local pinned
	pinned="$(pinned_version)"
	if [ -n "$pinned" ]; then
		normalize_tag "$pinned"
		return
	fi

	log_info "Fetching latest release..."
	local latest_url="https://api.github.com/repos/${REPO}/releases/latest"
	local tmp
	tmp="$(mktemp)"
	if ! download "$latest_url" "$tmp"; then
		rm -f "$tmp"
		log_error "Failed to query GitHub Releases"
		echo "Set a version explicitly: CLAUDE_CURSOR_PROXY_VERSION=v0.1.22 bash install.sh"
		exit 1
	fi
	# Prefer python/jq if present; fall back to sed
	local version=""
	if command -v jq >/dev/null 2>&1; then
		version="$(jq -r '.tag_name // empty' "$tmp")"
	elif command -v python3 >/dev/null 2>&1; then
		version="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("tag_name",""))' "$tmp")"
	else
		version="$(grep '"tag_name"' "$tmp" | head -1 | sed -E 's/.*"tag_name":[[:space:]]*"([^"]+)".*/\1/')"
	fi
	rm -f "$tmp"

	if [ -z "$version" ] || [ "$version" = "null" ]; then
		log_error "No GitHub release found for ${REPO}"
		echo "Create a release (tag vX.Y.Z) first, or pin CLAUDE_CURSOR_PROXY_VERSION."
		exit 1
	fi
	normalize_tag "$version"
}

skip_checksum() {
	[ "${CLAUDE_CURSOR_PROXY_INSECURE_SKIP_CHECKSUM:-}" = "1" ] \
		|| [ "${CLAUDE_CURSOR_BRIDGE_INSECURE_SKIP_CHECKSUM:-}" = "1" ] \
		|| [ "${CLAUDE_CODE_PROXY_INSECURE_SKIP_CHECKSUM:-}" = "1" ]
}

verify_checksum() {
	local checksum_file="$1"
	if skip_checksum; then
		log_warning "Skipping checksum verification (INSECURE_SKIP_CHECKSUM=1)"
		return 0
	fi
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum -c "$checksum_file" >/dev/null
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 -c "$checksum_file" >/dev/null
	else
		log_error "Neither sha256sum nor shasum found; cannot verify download"
		echo "Install coreutils/shasum, or set CLAUDE_CURSOR_PROXY_INSECURE_SKIP_CHECKSUM=1 (not recommended)."
		exit 1
	fi
}

ad_hoc_codesign() {
	local binary="$1"
	[[ "$(uname -s)" == "Darwin" ]] || return 0

	if command -v xattr >/dev/null 2>&1; then
		xattr -d com.apple.quarantine "$binary" 2>/dev/null || true
	fi
	if command -v codesign >/dev/null 2>&1; then
		# Fresh ad-hoc signature — copying unsigned/broken Mach-O can yield "Killed: 9"
		codesign --remove-signature "$binary" 2>/dev/null || true
		if ! codesign --force -s - "$binary" 2>/dev/null; then
			log_warning "codesign failed; if the binary dies with Killed: 9, run:"
			echo "  codesign --force -s - \"$binary\""
		fi
	fi
}

install_from_release() {
	local platform="$1"
	local tmp_dir
	tmp_dir="$(mktemp -d)"
	# shellcheck disable=SC2064
	trap "rm -rf '$tmp_dir'" EXIT

	local version
	version="$(resolve_version)"
	local bare_version="${version#v}"
	log_info "Installing version: ${version}"

	# Prefer versioned asset; fall back to platform-only alias
	local archive_versioned="${BIN_NAME}-${bare_version}-${platform}.tar.gz"
	local archive_alias="${BIN_NAME}-${platform}.tar.gz"
	local archive_name=""

	cd "$tmp_dir"
	if download "https://github.com/${REPO}/releases/download/${version}/${archive_versioned}" "$archive_versioned" 2>/dev/null; then
		archive_name="$archive_versioned"
	elif download "https://github.com/${REPO}/releases/download/${version}/${archive_alias}" "$archive_alias" 2>/dev/null; then
		archive_name="$archive_alias"
	else
		log_error "Download failed for ${platform}"
		echo "Tried:"
		echo "  https://github.com/${REPO}/releases/download/${version}/${archive_versioned}"
		echo "  https://github.com/${REPO}/releases/download/${version}/${archive_alias}"
		echo "Releases: https://github.com/${REPO}/releases"
		echo "Note: Linux prebuilts are glibc (ubuntu-latest / 24.04). Alpine/musl users should build from source."
		exit 1
	fi
	log_info "Downloaded ${archive_name}"

	local checksum_file="${archive_name%.tar.gz}.sha256"
	local checksum_url="https://github.com/${REPO}/releases/download/${version}/${checksum_file}"
	# Also try platform-only checksum if versioned missing
	if ! download "$checksum_url" "$checksum_file" 2>/dev/null; then
		checksum_file="${BIN_NAME}-${platform}.sha256"
		checksum_url="https://github.com/${REPO}/releases/download/${version}/${checksum_file}"
		if ! download "$checksum_url" "$checksum_file" 2>/dev/null; then
			log_error "Failed to download checksum file"
			exit 1
		fi
	fi

	log_info "Verifying checksum..."
	if ! verify_checksum "$checksum_file"; then
		log_error "Checksum verification failed"
		exit 1
	fi
	log_success "Checksum verified"

	log_info "Extracting..."
	tar -xzf "$archive_name"
	if [ ! -f "${BIN_NAME}" ]; then
		log_error "Archive did not contain ${BIN_NAME}"
		exit 1
	fi
	chmod +x "${BIN_NAME}"

	local install_dir="${CLAUDE_CURSOR_PROXY_INSTALL_DIR:-${CLAUDE_CURSOR_BRIDGE_INSTALL_DIR:-${CLAUDE_CODE_PROXY_INSTALL_DIR:-}}}"
	if [ -z "$install_dir" ]; then
		if [ -w /usr/local/bin ] 2>/dev/null; then
			install_dir="/usr/local/bin"
		else
			install_dir="${HOME}/.local/bin"
			mkdir -p "$install_dir"
		fi
	else
		mkdir -p "$install_dir"
	fi

	log_info "Installing to ${install_dir}..."
	local dest="${install_dir}/${BIN_NAME}"
	local tmp_binary="${dest}.tmp.$$"

	if [ -w "$install_dir" ]; then
		cp "${BIN_NAME}" "$tmp_binary"
		chmod +x "$tmp_binary"
		ad_hoc_codesign "$tmp_binary"
		mv -f "$tmp_binary" "$dest"
	else
		need_cmd sudo
		sudo cp "${BIN_NAME}" "$tmp_binary"
		sudo chmod +x "$tmp_binary"
		# codesign before final move when possible
		if [[ "$(uname -s)" == "Darwin" ]] && command -v codesign >/dev/null 2>&1; then
			sudo codesign --remove-signature "$tmp_binary" 2>/dev/null || true
			sudo codesign --force -s - "$tmp_binary" 2>/dev/null || true
		fi
		sudo mv -f "$tmp_binary" "$dest"
		ad_hoc_codesign "$dest"
	fi

	# Re-sign final path on macOS (mv can invalidate in some edge cases)
	ad_hoc_codesign "$dest"

	log_success "${BIN_NAME} installed to ${dest}"

	# Optional compatibility symlinks for previous binary names
	for old_name in claude-cursor-bridge claude-code-proxy; do
		link="${install_dir}/${old_name}"
		if [ -e "$link" ] && [ ! -L "$link" ]; then
			log_warning "Skipping symlink ${link} (exists and is not a symlink)"
			continue
		fi
		if [ -w "$install_dir" ]; then
			ln -sfn "${BIN_NAME}" "$link" 2>/dev/null || true
		elif command -v sudo >/dev/null 2>&1; then
			sudo ln -sfn "${BIN_NAME}" "$link" 2>/dev/null || true
		fi
	done

	if [[ ":${PATH}:" != *":${install_dir}:"* ]]; then
		log_warning "${install_dir} is not in your PATH"
		echo ""
		echo "Add this to your shell profile (~/.zshrc or ~/.bashrc):"
		echo "  export PATH=\"\$PATH:${install_dir}\""
		echo ""
	fi

	INSTALL_DIR="$install_dir"
	cd - >/dev/null || true
}

print_next_steps() {
	local install_dir="$1"
	local bin="${install_dir}/${BIN_NAME}"

	log_success "${BIN_NAME} is ready"
	echo ""
	"$bin" --version
	echo ""
	echo "Next steps:"
	echo "  1. Log in to Cursor:"
	echo "       ${BIN_NAME} cursor auth login"
	echo "  2. Start the bridge:"
	echo "       ${BIN_NAME} serve"
	echo "  3. Point Claude Code at it (Fable 5):"
	echo "       export ANTHROPIC_BASE_URL=http://127.0.0.1:18765"
	echo "       export ANTHROPIC_AUTH_TOKEN=unused"
	echo "       export ANTHROPIC_MODEL=claude-fable-5[1m]"
	echo "       export ANTHROPIC_SMALL_FAST_MODEL=claude-fable-5[1m]"
	echo "       export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1"
	echo "       export CLAUDE_CODE_DISABLE_NONSTREAMING_FALLBACK=1"
	echo "       claude"
	echo ""
	echo "Docs: https://github.com/${REPO}"
	echo ""
}

main() {
	echo ""
	echo "${BIN_NAME} installer"
	echo "repo: ${REPO}"
	echo ""

	log_info "Detecting platform..."
	local platform
	platform="$(detect_platform)"
	log_info "Platform: ${platform}"

	install_from_release "$platform"
	print_next_steps "$INSTALL_DIR"
}

main "$@"
