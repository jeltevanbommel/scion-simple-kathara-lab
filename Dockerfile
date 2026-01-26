FROM kathara/base


# Install SCION from GitHub release
ARG DEBIAN_FRONTEND="noninteractive"
ARG TARGETARCH

WORKDIR /tmp

RUN wget https://github.com/scionproto/scion/releases/download/v0.12.0/scion_0.12.0_deb_$TARGETARCH.tar.gz && \
    tar xfz scion_0.12.0_deb_$TARGETARCH.tar.gz && \
    apt install ./scion*.deb && \
    apt clean && \
    rm -rf /tmp/* /var/lib/apt/lists/* /var/tmp/*

# Tools for TC eBPF + NFQUEUE userspace filtering
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    bison \
    build-essential \
    clang \
    flex \
    golang-go \
    iproute2 \
    libc6-dev \
    linux-libc-dev \
    libelf-dev \
    libmnl-dev \
    libcap-dev \
    libbpf-dev \
    libnetfilter-queue-dev \
    llvm \
    nftables \
    bpftool \
    pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Build iproute2 with libelf support to ensure tc can load BPF programs.
ARG IPROUTE2_VERSION=6.9.0
RUN wget https://mirrors.edge.kernel.org/pub/linux/utils/net/iproute2/iproute2-${IPROUTE2_VERSION}.tar.xz && \
    tar xf iproute2-${IPROUTE2_VERSION}.tar.xz && \
    make -C iproute2-${IPROUTE2_VERSION} && \
    make -C iproute2-${IPROUTE2_VERSION} install && \
    rm -rf iproute2-${IPROUTE2_VERSION} iproute2-${IPROUTE2_VERSION}.tar.xz

# Strip templating from systemd service files, systemd-replacement-script does not support it
RUN mv /usr/lib/systemd/system/scion-router\@.service /usr/lib/systemd/system/scion-router.service && \
    sed -i 's/%i/br/g' /usr/lib/systemd/system/scion-router.service && \
    mv /usr/lib/systemd/system/scion-control\@.service /usr/lib/systemd/system/scion-control.service && \
    sed -i 's/%i/cs/g' /usr/lib/systemd/system/scion-control.service

WORKDIR /
