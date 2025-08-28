# To Do

* Look into improving error propagation so that toast messages can be displayed to the user if generation fails
  * Main thread should never panic in response to a user action!
  * Update handler(s) to return axum::http::StatusCode, other errors will need to propagate through to handler
  * Use mapping/and_then to daisy-chain fns which may error out, avoids deeply nested match statements
* Apply global timeout for route creation
* Split out API calls into
  * Request route (existing call) - routes/request
    * Receive route config
    * Return request ID
  * Get request status - routes/status
    * Receive request ID
    * Return status details (iteration, current dist, avg dist, percentage)
  * Retrieve route - routes/retrieve
    * Receive request ID
    * Return routes (existing output)

# Questions

* How to get data from one process into another?
  * Redis?
    * Potentially lots of writes to global cache, performance overhead (mitigate by updating every nth iteration?)
  * Use message channels to provide updates?
    * Assign thread ID, keep channel receiver in hashmap? Latest status fetched on-demand.
    * May be difficult to scale beyond one main thread, better to have global visibility of ids
* How to set axum up to work with load balancer in k8s?
* Any analogue to schemas in Redis? Preferable to segregate statuses and completed routes, set different expiration policies on each.


# Notes

* No need to share graph data between threads
* Look into where the `move` keyword can be used, forces transfer of ownership over borrow

# API Changes

Host Process(es)

* Acts as API endpoint
  * Spawn call to route creation endpoint
    * Return thread ID to user, retain in-thread
  * Retrieve details of ongoing route creation requests
    * Route creation process periodically writes out status to redis, key is thread ID
    * Host process can pull status from redis as required
  * Retrieve details of completed routes
    * As before, creation process writes generated routes out to redis
    * Host process can pull from redis as required

# Proposed Architecture

* Redis as global cache
  * For local dev share with webapp backend, for containers spin up separate instance
* If possible, look into scaling strategies for Axum with k8s
  * Strategy may not be any different from Betelgeuse, port bindings managed by k8s/docker

Design for local dev, assume port bindings won't need to be configured dynamically within Rust

