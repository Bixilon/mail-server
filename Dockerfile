FROM alpine:latest as builder

RUN apk add --no-cache g++ rustup make clang clang-libclang clang-static openssl-dev openssl-libs-static
RUN echo "1" | rustup-init
WORKDIR /build


ARG TARGET="x86_64-unknown-linux-musl"
ARG FEATURES="mysql s3"
RUN source "$HOME/.cargo/env" && rustup target add "$TARGET"
RUN source "$HOME/.cargo/env" && rustup component add rustfmt
    
COPY . .
RUN source "$HOME/.cargo/env" && cargo build --target "$TARGET" --release -p mail-server --no-default-features --features "$FEATURES"
RUN source "$HOME/.cargo/env" && cargo build --target "$TARGET" --release -p stalwart-cli
RUN cp -r "/build/target/$TARGET/release" "/output"
# RUN source "$HOME/.cargo/env" && cargo test --target "$TARGET" --no-default-features --features "$FEATURES"

FROM alpine:latest as test
COPY --from=builder /output/stalwart-mail /usr/local/bin
COPY --from=builder /output/stalwart-cli /usr/local/bin
RUN chmod -R 755 /usr/local/bin/stalwart-mail /usr/local/bin/stalwart-cli
RUN /usr/local/bin/stalwart-mail -V
RUN /usr/local/bin/stalwart-cli -V



FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder /output/stalwart-mail /usr/local/bin
COPY --from=builder /output/stalwart-cli /usr/local/bin
RUN chmod -R 755 /usr/local/bin/stalwart-mail /usr/local/bin/stalwart-cli
