# Next Steps

* Get code connected up to python app, move into main repo
* Strip out python route generation code, update dataclasses to bring in all data returned by the API (e.g. bounds, centre point)
* Write tests for all code required to generate the graph
* API needs fleshing out, improve status codes returned
* Need to get route generation into a more reliable state
* Any way to improve how routes get knocked out to prevent processing of invalid routes?
* Check against fell_finder to see if any logic has been missed
* Improve code structure
  * Split some logic out from Candidate struct, set logic for taking steps up to accept candidate, config & graph as arguments
* Try to make code more idiomatic
* Bring back progress bars for the API? Could be done with async calls, create a request ID then poll to get the current distance.

# Future optimisations

* Data load can probably be sped up by taking ownership of the data loaded by sqlx directly instead of cloning
* Only store points in candidates, unpack geometry only for completed routes
* Check for other areas where threading can be implemented

# Profiling
echo '1' | sudo tee /proc/sys/kernel/perf_event_paranoid
echo '1024' | sudo tee /proc/sys/kernel/perf_event_mlock_kb
cargo build --profile profiling
samply record ./target/profiling/rust-sandbox

# API

curl -X GET "http://localhost:8000/loop?lon=-1.3840097&lat=50.9695368&route_mode='hilly'&max_candidates=2048&target_distance=5000&highways='secondary'&surfaces='paved'" -H "Content-Type: application/json"
