# syntax=docker/dockerfile:1

FROM debian:bookworm AS build

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt install -y --no-install-recommends \
        build-essential cmake git ca-certificates wget lsb-release gnupg doxygen graphviz \
    && wget -O /tmp/apache-arrow-apt-source.deb \
        "https://packages.apache.org/artifactory/arrow/debian/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb" \
    && apt install -y --no-install-recommends /tmp/apache-arrow-apt-source.deb \
    && apt-get update \
    && apt install -y --no-install-recommends \
        libosmium2-dev libprotozero-dev libexpat1-dev \
        libbz2-dev zlib1g-dev libzstd-dev \
        libarrow-dev libarrow-compute-dev libparquet-dev \
        libgtest-dev

WORKDIR /src
COPY CMakeLists.txt ./
COPY src ./src
COPY tests ./tests

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)"

# A failing test fails the image build.
RUN ctest --test-dir build --output-on-failure

# Collect the binary together with every shared library it actually links
# against (via ldd), instead of re-declaring versioned Debian package names
# in the runtime stage. Guessing package names such as libarrowNNNN has
# proven fragile across Arrow version bumps; this approach tracks whatever
# the build stage actually produced.
RUN mkdir -p /out/lib \
    && cp build/osh_change_index /out/osh_change_index \
    && ldd /out/osh_change_index \
        | awk '$2 == "=>" && $3 ~ /^\// { print $3 }' \
        | sort -u \
        | xargs -I{} cp -L {} /out/lib/

FROM debian:bookworm-slim AS runtime

COPY --from=build /out/lib/ /usr/local/lib/
COPY --from=build /out/osh_change_index /usr/local/bin/osh_change_index
RUN ldconfig

WORKDIR /data

# No fixed ENTRYPOINT/CMD: invoke explicitly at run time, e.g.
#   docker run --rm -v ... osh_change_index osh_change_index --input ... --node-cache ... --output-dir ...
