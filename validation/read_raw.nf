include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromPath("*.parquet").splitParquet()
        | view