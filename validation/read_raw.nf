include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromPath("data/presidents.parquet").splitParquet(by:1)
        | view
