.PHONY: help fmt fmt-check clippy test audit ci clean

help:
	@awk 'BEGIN{FS=":.*##"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Apply rustfmt
	cargo fmt --all

fmt-check: ## Check formatting (CI gate)
	cargo fmt --all --check

clippy: ## Lint with warnings denied (CI gate)
	cargo clippy --workspace --all-targets --locked -- -D warnings

test: ## Run workspace tests (CI gate)
	cargo test --workspace --locked

audit: ## Check advisories, honoring .cargo/audit.toml (CI gate)
	cargo audit

ci: fmt-check clippy test audit ## Run the full CI matrix locally

clean: ## Remove target/
	cargo clean
