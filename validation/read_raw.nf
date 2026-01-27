include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromPath("*.parquet").splitParquet(by:1)
        | view
