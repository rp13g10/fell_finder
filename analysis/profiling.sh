echo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid;
echo '1024' | sudo tee /proc/sys/kernel/perf_event_mlock_kb;
cargo build --profile profiling;
uv run --env-file ../.env samply record ../target/profiling/fell-finder