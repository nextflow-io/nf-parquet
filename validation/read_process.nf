include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*


process processParquetChunk {
    input:
    val(input_chunk_parquet)
    output:
    stdout 
    script:
    """
        echo ${input_chunk_parquet.size()}
    """
}

workflow{

channel.fromPath("data/customs.parquet").splitParquet(by:100)
        | processParquetChunk
        | view

}
