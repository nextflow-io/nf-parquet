include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromPath("test*.parquet").splitParquet( record: SingleRecord)
        | view