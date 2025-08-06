# ESeMan (Event SEquence MANager)

A hierarchical data management system to support querying of event sequence data for parallel timeline charts. It utilizes KD-Tree and agglomorative clustering to store event sequences. 



![ESeMan Library](resources/framework.png)
This figure presents ESeMan works. ESeMan provides support for low latency interactions in parallel timeline charts through fast querying. It ingests and cleans event sequence data, building indices on the events and then serving summarized events to match data queries. These summarized events provide pixel accuracy with less data sent. Stronger summarization can trade accuracy for latency.

