# `deltapack server` (and `client`) as an image: the release binary on Debian slim. TLS is rustls
# with the Mozilla roots built in, so the image needs no CA bundle.
ARG BUILDER=rust:1.95.0-bookworm
FROM ${BUILDER} AS build
WORKDIR /src
COPY rust-toolchain.toml .
RUN rustup show active-toolchain
COPY . .
RUN cargo build --release --locked --features proxy && strip target/release/deltapack

FROM debian:bookworm-slim
COPY --from=build /src/target/release/deltapack /usr/local/bin/deltapack
# a record directory the unprivileged user can write, which a named volume takes on
RUN install -d -o 65534 -g 65534 /data
USER 65534
EXPOSE 8080
ENTRYPOINT ["deltapack"]
CMD ["server", "--help"]
