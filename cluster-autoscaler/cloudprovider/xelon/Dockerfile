# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM --platform=$BUILDPLATFORM golang:1.26 AS builder

WORKDIR /workspace
COPY . .

ARG TARGETARCH
ARG XELON_VERSION=1.35.2-xelon.1
ARG XELON_REVISION

RUN test -n "$XELON_REVISION" && \
    CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" go build \
      -tags xelon \
      -ldflags "-s -w -X k8s.io/autoscaler/cluster-autoscaler/cloudprovider/xelon.BuildVersion=${XELON_VERSION} -X k8s.io/autoscaler/cluster-autoscaler/cloudprovider/xelon.BuildRevision=${XELON_REVISION}" \
      -o /cluster-autoscaler .

FROM gcr.io/distroless/static:nonroot

ARG XELON_VERSION=1.35.2-xelon.1
ARG XELON_REVISION

LABEL org.opencontainers.image.title="Xelon Cluster Autoscaler" \
      org.opencontainers.image.version="$XELON_VERSION" \
      org.opencontainers.image.revision="$XELON_REVISION" \
      org.opencontainers.image.source="https://github.com/Xelon-AG/autoscaler" \
      io.xelon.cluster-autoscaler.upstream.version="1.35.2" \
      io.xelon.cluster-autoscaler.upstream.revision="2d42588803c71fe9b35dcd9e3669ac6bb550ca22"

COPY --from=builder /cluster-autoscaler /cluster-autoscaler
USER 65532:65532
ENTRYPOINT ["/cluster-autoscaler"]
