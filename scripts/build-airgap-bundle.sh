#!/bin/sh
set -eu
version=${1:-dev}
out=${2:-dist}
mkdir -p "$out"
docker build -t "deinopis:$version" .
docker build -f docker/sim/Dockerfile -t "deinopis-logsim:$version" .
docker save "deinopis:$version" "deinopis-logsim:$version" -o "$out/deinopis-images-$version.tar"
cp docker-compose.yaml "$out/docker-compose.yaml"
mkdir -p "$out/packs"
cp packs/*.yaml "$out/packs/"
cp -R schemas sources sinks "$out/"
if command -v sha256sum >/dev/null 2>&1; then
	(cd "$out" && sha256sum "deinopis-images-$version.tar" docker-compose.yaml > SHA256SUMS)
else
	(cd "$out" && shasum -a 256 "deinopis-images-$version.tar" docker-compose.yaml > SHA256SUMS)
fi
tar -C "$out" -czf "$out/deinopis-airgap-$version.tar.gz" "deinopis-images-$version.tar" docker-compose.yaml packs schemas sources sinks SHA256SUMS
