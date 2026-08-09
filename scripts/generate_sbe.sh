#!/usr/bin/env bash
set -euo pipefail

SBE_VERSION="${SBE_VERSION:-1.39.0}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCHEMA="${ROOT_DIR}/protocol/ignition.xml"
OUT_DIR="${ROOT_DIR}/protocol/generated"
TMP_OUT_DIR="${ROOT_DIR}/protocol/.generated-tmp"
TOOLS_DIR="${ROOT_DIR}/target/tools"
SBE_JAR="${SBE_JAR:-${TOOLS_DIR}/sbe-all-${SBE_VERSION}.jar}"
SBE_URL="https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/${SBE_VERSION}/sbe-all-${SBE_VERSION}.jar"

mkdir -p "${TOOLS_DIR}"

if [[ ! -f "${SBE_JAR}" ]]; then
  curl -fsSLo "${SBE_JAR}" "${SBE_URL}"
fi

rm -rf "${OUT_DIR}" "${TMP_OUT_DIR}"
mkdir -p "${TMP_OUT_DIR}"

java \
  -Dsbe.target.language=Rust \
  -Dsbe.output.dir="${TMP_OUT_DIR}" \
  -Dsbe.target.namespace=ignition_protocol_generated \
  -jar "${SBE_JAR}" \
  "${SCHEMA}"

mv "${TMP_OUT_DIR}/ignition_protocol_generated" "${OUT_DIR}"
rmdir "${TMP_OUT_DIR}"

cargo fmt --manifest-path "${OUT_DIR}/Cargo.toml"
