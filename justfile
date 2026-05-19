set shell := ["zsh", "-cu"]

default: smoke

check:
    cargo check

check-core:
    cargo check --no-default-features

check-full:
    cargo check --no-default-features --features full

check-examples:
    cargo check --examples
    cargo check --no-default-features --features full --examples

smoke: check-core check check-full check-examples

package-list:
    cargo package --allow-dirty --list

tree:
    cargo tree --no-default-features --depth 1
    cargo tree --features full --depth 1
