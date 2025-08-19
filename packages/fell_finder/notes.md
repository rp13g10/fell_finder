# Next Steps

* Bring back progress bars for the API? Could be done with async calls, create a request ID then poll to get the current distance.

# Future optimisations

* Data load can probably be sped up by taking ownership of the data loaded by sqlx directly instead of cloning
* Only store points in candidates, unpack geometry only for completed routes
* Check for other areas where threading can be implemented

# Profiling
echo '-1' | sudo tee /proc/sys/kernel/perf_event_paranoid
echo '1024' | sudo tee /proc/sys/kernel/perf_event_mlock_kb
cargo build --profile profiling
samply record ./target/profiling/rust-sandbox

# API

curl -X GET "http://localhost:8000/loop?lon=-1.3840097&lat=50.9695368&route_mode='hilly'&max_candidates=2048&target_distance=5000&highways='secondary'&surfaces='paved'" -H "Content-Type: application/json"
