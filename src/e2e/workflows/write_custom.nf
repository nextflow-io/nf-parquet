include { splitParquet; toParquet } from 'plugin/nf-parquet'

import myrecords.*

numrecords = (params.numrecords ?: 10) as int

workflow{
    main:
    channel.of(1..numrecords)
            .map( {
                new CustomRecord(it, "the $it record", it, it, it)
            })
            .toParquet("${workDir}/demo.parquet",[record: CustomRecord])
    emit:
        "${workDir}/demo.parquet"
}