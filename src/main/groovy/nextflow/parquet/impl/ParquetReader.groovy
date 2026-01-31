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
    private List batchList = []
    private int sizeBatch = 0

    ParquetReader(RecordConsumer recordAware, Map params) {
        if( recordAware == null){
            throw new IllegalArgumentException("RecordAware is required")
        }
        this.recordAware = recordAware
        parseArgs(params)
    }

    private List<String> ARGUMENTS = [
            'record',
            'by'
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
        sizeBatch = params.containsKey('by') && "$params.by".isNumber() ? params.by as int : 0
    }

    void readFile(File source) {
        try {
            log.debug "Start reading $source, with projection ${clazz ?: 'raw'}"

            boolean stopped = false
            final reader = new CarpetReader(source, clazz ?: Map)
            for (def record : reader) {
                batchList.add(record)
                if (batchList.size() >= sizeBatch) {
                    stopped = recordAware.next( sizeBatch == 0 ? batchList.first() : batchList.toArray() )
                    batchList.clear()
                    if( stopped ){
                        break
                    }
                }
            }
            if (batchList.size()) {
                if( !stopped ) {
                    recordAware.next(sizeBatch == 0 ? batchList.first() : batchList.toArray())
                }
                batchList.clear()
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Error while reading Parquet file - cause: ${e.message ?: e}", e)
        }
    }


}
