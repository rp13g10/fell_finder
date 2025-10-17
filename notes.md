# To Do

* Set up some different backends for route pruning for easier testing of approaches
* Add in support for step validation using dijkstra's algorithm

## Binning

Options to be supported

* Bin by current position
* Bin by average position (midpoint)
  * Ensuring that tracking of average is implemented in O(N)

Trait `binner` will need to be defined
Struct `CurrentBinner`
Struct `MidpointBinner`

## Pruning

Options to be supported

* Route selection based on elevation
* Route selection based on ratio
* Select with fuzzy deduplication
* Select based purely on chosen metric

Trait `sorter` will need to be defined
Struct `ElevationSorter`
Struct `RatioSorter`
Trait `selector` will need to be defined
Struct `FuzzySelector`
Struct `NaiveSelector`

Maybe wrap everything into overarching struct?