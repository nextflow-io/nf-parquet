package nextflow.parquet.impl

import com.jerolba.carpet.CarpetReader
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j


/**
 * Implements a parquet files reader
 *
 * @author Jorge Aguilera <jorge@edn.es>
 */
@Slf4j
@CompileStatic
class ParquetReader {
    private RecordConsumer recordAware
    private Class<Record> clazz

    ParquetReader(RecordConsumer recordAware, Map params) {
        if( recordAware == null){
            throw new IllegalArgumentException("RecordAware is required")
        }
        this.recordAware = recordAware
        parseArgs(params)
    }

    private List<String> ARGUMENTS = [
            'record',
    ]

    void parseArgs(Map params){
        for(def key : params.keySet()){
            if( !ARGUMENTS.contains(key) ){
                throw new IllegalArgumentException("Unknow ${key} arguments. Posible values ${ARGUMENTS}")
            }
        }
        if (params.record) {
            if (!(params.record instanceof Class<Record>)) {
                throw new IllegalArgumentException("A Record.class is required. Class provided $params.record")
            }
            this.clazz = params.record as Class<Record>
        }
    }

    void readFile(File source) {
        try {
            log.debug "Start reading $source, with projection ${clazz ?: 'raw'}"

            boolean stopped = false
            final reader = new CarpetReader(source, clazz ?: Map)
            for (def record : reader) {
                stopped = !recordAware.wantMore(record )
                if( stopped ){
                    break
                }
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Error while reading Parquet file - cause: ${e.message ?: e}", e)
        }
    }


}
