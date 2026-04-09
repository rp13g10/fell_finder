======================
Overview - Fell Loader
======================

The Fell Loader component exists to transform raw datasets into something which can be used to power the route creation algorithm. Due to the size of the raw datasets, this is the most computationally expensive part of the entire application.

Source Data
===========

OSM
---

.. image:: assets/openstreetmap.png

`Open Street Map <https://www.openstreetmap.org/about>`_ (OSM) is the de-facto standard for map data in open source applications. It provides a source of map data which spans the globe, and is free of charge to use.

For the purposes of creating running routes, we are mostly interested in using the `nodes <https://wiki.openstreetmap.org/wiki/Node>` and `ways <https://wiki.openstreetmap.org/wiki/Way>`_ in the OSM dataset. This provides a representation of the network of the roads/paths in the UK in the form of a massive network graph. With a little leg-work, this can be refined into a format we can use for route creation. You can use `openstreetmap.org <https://www.openstreetmap.org/>`_ to explore the OSM dataset in an interactive window.

.. image:: assets/osm_node.png

In the context of the OSM dataset, a node represents a point on the map. Its position is recorded with a latitude and a longitude, and it has a unique numerical identifier.

.. image:: assets/osm_way.png

A way is a collection of nodes which makes up a line on the map. The ways which we are interested in represent roads & paths, although other types are present in the raw data (boundaries, buildings, etc).

.. image:: assets/network_graph.png

Between the nodes and ways in the OSM dataset, we have all of the data required to form a network graph. Each way forms one or more edges, while nodes can be used without modification.

LIDAR
-----

.. image:: assets/lidar.png

The `LIDAR <https://environment.data.gov.uk/DefraDataDownload/?Mode=survey>`_ dataset provided by DEFRA is one of a few potential sources of elevation data for the UK. It is freely available to download, and with a 1m resolution is the most detailed dataset available.

.. image:: assets/lidar_array.png

This dataset is made available as a collection of 'tif' (image) files, which can be loaded in as arrays. These arrays typically cover an area of 5km^2, with each value in the array corresponding to the elevation at a point.

.. image:: assets/bng.png

The points in these arrays are aligned to the `British National Grid <>` (BNG) coordinate system. Along with the .tif files, each of the .zip files containing elevation data also contains a .xml file which defines its corner points. This information is sufficient to determine the elevation for any given coordinate in the BNG.

Processing
==========

Significant processing is required to get data from its raw format into a state where it can be used to generate routes. A high-level view of the transformations applied is given below.

Landing
-------

Data in the landing layer is presented in a raw tabular format. The purpose of this layer is to get all of the raw data into a format which can be easily processed, with further cleaning deferred until the later stages.

Lidar
^^^^^

This dataset is very large, with potentially thousands of files to be processed. The logic outlined below is carried out recursively across all available source files. Each file is assigned an ID based on its name; for example, 'lidar_composite_dtm-2022-1-SU41ne.zip' is given the ID 'SU41ne'. Once processed, its contents are written out to 'SU41ne.parquet' in the landing layer.

.. code-block:: xml

    <spatRepInfo>
        <Georect>
            ...
            <cornerPts>
                <pos Sync="TRUE">445000.000000 115000.000000</pos>
            </cornerPts>
            <cornerPts>
                <pos Sync="TRUE">445000.000000 120000.000000</pos>
            </cornerPts>
            <cornerPts>
                <pos Sync="TRUE">450000.000000 120000.000000</pos>
            </cornerPts>
            <cornerPts>
                <pos Sync="TRUE">450000.000000 115000.000000</pos>
            </cornerPts>
            <centerPt>
                <pos Sync="TRUE">447500.000000 117500.000000</pos>
            </centerPt>
            ...
        </Georect>
    </spatRepInfo>


First, extract the corner points of the current archive from the included .xml file. The corner points of each file are provided, with coordinates given using the BNG system. For the example shown, the file covers eastings from 445000 to 450000, and northings from 115000 to 120000.

Due to the size of the dataset, the boundaries of each file are also recorded in a separate (smaller) export. This forms a lookup table which can be used to determine the files which contain elevation data for a given area.

.. image:: assets/lidar_array.png

Next, read the included .tif file in as a numpy array. This starts life as a 2d array of size 5000*5000. However, we can calculate the easting and northing for each element in the array as the corner points have been determined.

.. image:: assets/lidar_df.png

The array is flattened to return a dataframe with 25m records, recording the elevation for each easting & northing covered by the array. This is the format which is stored in the landing layer.

OSM
^^^

.. image:: assets/raw_ways.png

The raw OSM data is presented in a binary format, which must first be converted into something easier to work with. This initial conversion is handled by `OSM Parquetizer <TODO>`_, which exports parquet files containing details of the nodes and the ways in the dataset.

.. code-block::

    input = [{key=tag_1, value=value_1}, {key=tag_2, value=value_2}]
    target = {tag_1: value_1, tag_2: value_2}

Tags contain most of the information which this app needs to generate running routes, providing information about the type of road/path, its surface, max speeds, etc. To make these easier to process, they are converted from the raw format shown above into a more standard mapping. To limit the scope of the data being ingested, any records which do not have a 'highway' tag are discarded. These records correspond to other lines on the map such as boundaries, but do not form part of the road/path network.

.. code-block::

    input = [1, 2, 3, 4, 5]
    target = [(1, 2), (2, 3), (3, 4), (4, 5)]

In preparation for the upcoming join with the LIDAR dataset, a table containing all of the edges in the map must also be created. The geometry of each way is encoded as a list of node IDs. These are converted into source-destination pairs.

.. image:: assets/bng.png

Finally, both nodes and edges are assigned BNG coordinates in order to join them with the LIDAR dataset. Latitude and longitude are provided as part of the map data, and conversion is handled by the `bng-latlon <TODO>`_ package.

Staging
-------

.. image:: assets/lidar_size.png

By far the most time-consuming stage in the data ingestion process is determining the elevation for each point on the map. Superficially, this is a simple join between two datasets. However, at over 400 GB, the LIDAR dataset in particular is large enough to present a challenge to consumer-grade hardware.

The changeable nature of the OSM dataset must also be considered. Users are likely to want to refresh the data available to the app periodically, so an approach is required which enables incremental updates to be carried out. While new roads/paths are always being mapped, it can safely be assumed that the bulk of the map will remain unchanged month-on-month. Re-processing the entire dataset represents a significant waste of resource.

.. image:: assets/delta_table.png

To enable incremental loads, the staging layer is stored using delta tables. This allows for the application of record updates/deletion in a parquet-based table. Determining the records which must be updated is relatively straightforward:

    * Remove records in the existing dataset which are not present in the (newer) OSM dataset being loaded
    * Add records in the dataset being loaded which are not present in the existing dataset
    * Update records which have been modified since the last data refresh
        * The OSM dataset contains a timestamp showing last modified dates
    * Update records which have not yet been joined onto the LIDAR dataset (i.e. elevation is NULL)

This logic is applied to both the nodes and the edges datasets. Once the records which must be updated have been identified, they are joined onto the LIDAR dataset to populate them with elevation data.

Nodes
^^^^^

.. image:: assets/bng_partitions.png

As each node is a single point on the map, tagging them with elevation data can effectively be done as a join operation. To reduce the amount of data which spills to disk while carrying out the join, the number of LIDAR files read in must be minimised. This can (crudely) be achieved by ordering the nodes by their eastings and northings, divided by 5000 to mirror the size of the LIDAR files. After ordering, the top N records are then used to form a single 'batch' of data.

As the bounds of each LIDAR file are known, we may limit the elevation data loaded to include only those files which overlap with the current batch of OSM data being processed. Each batch is joined onto the LIDAR data based on the node's easting & northing, and the result appended to the staging layer.

Edges
^^^^^

.. image:: assets/bng_edge_chunking.png

As with the nodes dataset, edges are processed in batches and joined onto the LIDAR dataset to populate them with elevation data. However, as an edge represents a line rather than a point, the process becomes somewhat more involved.

.. image:: assets/bng_edge_size.png

First, the approximate length of each edge is determined based on the easting/northing of its source and destination. This known size is then used to determine the number of samples which must used to meet the target resolution. For instance, if the target resolution is 10 metres and the edge is 30 metres in length, 4 samples must be taken (at 0m, 10m, 20m and 30m).

.. image:: assets/edge_explode.png

With the sample count set, each edge must be converted into a series of points. This is achieved by stepping from edge source to destination, for example:

* An edge starts at point 1000, 1000 and ends at point 2000, 3000
* The target sample count is 3
* Point 1 must be 1000, 1000
* 2 samples must be taken. For eastings the step size is 500, for northings is is 1000.
* Point 2 is then 1500, 2000
* Point 3 is then 2000, 3000

Once the coordinates of each point have been determined, the join to the LIDAR dataset can be performed.

.. image:: assets/edges_landing.png

Collecting all of the samples for each edge results in an array of elevation readings. This can be used to calculate the elevation gain/loss for each edge in later stages.

Sanitised
---------

Optimised
---------

PostgreSQL
----------