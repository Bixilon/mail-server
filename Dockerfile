FROM alpine:latest as builder

RUN apk add --no-cache g++ rustup make clang clang-libclang clang-static openssl-dev openssl-libs-static
RUN echo "1" | rustup-init
WORKDIR /build


ARG TARGET="x86_64-unknown-linux-musl"
ARG FEATURES="mysql s3"
RUN source "$HOME/.cargo/env" && rustup target add "$TARGET"
RUN source "$HOME/.cargo/env" && rustup component add rustfmt
    
COPY . .
RUN source "$HOME/.cargo/env" && cargo build --target "$TARGET" --release -p stalwart --no-default-features --features "$FEATURES"
RUN source "$HOME/.cargo/env" && cargo build --target "$TARGET" --release -p stalwart-cli
RUN cp -r "/build/target/$TARGET/release" "/output"
# RUN source "$HOME/.cargo/env" && cargo test --target "$TARGET" --no-default-features --features "$FEATURES"


RUN apk add --no-cache libcap-setcap \
    && setcap CAP_NET_BIND_SERVICE=+eip /output/stalwart \
    && apk del libcap-setcap


FROM alpine:latest as test
COPY --from=builder --chmod=555 /output/stalwart /usr/local/bin/stalwart-mail
COPY --from=builder --chmod=555 /output/stalwart-cli /usr/local/bin
RUN /usr/local/bin/stalwart-mail -V
RUN /usr/local/bin/stalwart-cli -V



FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder --chmod=555 /output/stalwart /usr/local/bin/stalwart-mail
COPY --from=builder --chmod=555 /output/stalwart-cli /usr/local/bin
