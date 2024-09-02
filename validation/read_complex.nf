include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromPath("presidents.parquet").splitParquet()
        | view