dev:
    uv run maturin develop

test:
    uv run pytest -q

test-rust:
    cargo test

build:
    uv build

build-release:
    uv build --release

clean:
    rm -rf dist/ python/_lite_server/*.so target/
    cargo clean
