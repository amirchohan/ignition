#!/usr/bin/env bash
set -euo pipefail

SBE_VERSION="${SBE_VERSION:-1.39.0}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCHEMA="${ROOT_DIR}/protocol/ignition.xml"
OUT_DIR="${ROOT_DIR}/src/protocol/generated"
TOOLS_DIR="${ROOT_DIR}/target/tools"
SBE_JAR="${SBE_JAR:-${TOOLS_DIR}/sbe-all-${SBE_VERSION}.jar}"
SBE_URL="https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/${SBE_VERSION}/sbe-all-${SBE_VERSION}.jar"

mkdir -p "${TOOLS_DIR}" "${OUT_DIR}"

if [[ ! -f "${SBE_JAR}" ]]; then
  curl -fsSLo "${SBE_JAR}" "${SBE_URL}"
fi

rm -rf "${OUT_DIR:?}"/*

java \
  -Dsbe.target.language=Rust \
  -Dsbe.output.dir="${OUT_DIR}" \
  -Dsbe.target.namespace=ignition_protocol_generated \
  -jar "${SBE_JAR}" \
  "${SCHEMA}"

if [[ ! -f "${OUT_DIR}/mod.rs" ]]; then
  cat > "${OUT_DIR}/mod.rs" <<'EOF'
//! Generated SBE codecs.
//!
//! Files in this module are generated from `protocol/ignition.xml` and must not
//! be edited by hand. The handwritten facade in `crate::protocol::codec` is the
//! public integration point for the rest of the crate.

pub use ignition_protocol_generated as codecs;
EOF
fi

cargo fmt --manifest-path "${OUT_DIR}/ignition_protocol_generated/Cargo.toml"
rustfmt "${OUT_DIR}/mod.rs"
