package nextflow.parquet.impl

import com.jerolba.carpet.CarpetWriter
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Implements a parquet files writer
 *
 * @author Jorge Aguilera <jorge@edn.es>
 */
@Slf4j
@CompileStatic
class ParquetWriter implements Closeable {
    private Class<Record> clazz
    CarpetWriter writer
    private List batchList = []
    private int sizeBatch = 0

    ParquetWriter(String output, Map params) {
        if( output == null){
            throw new IllegalArgumentException("Parquet file is required")
        }
        parseArgs(params)
        this.clazz = params.record as Class<Record>
        var outputStream = new FileOutputStream(output)
        writer = new CarpetWriter<>(outputStream, this.clazz)
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
        if (!params.record || !(params.record instanceof Class<Record>)) {
            throw new IllegalArgumentException("A Record.class is required. Class provided $params.record")
        }
        sizeBatch = params.containsKey('by') && "$params.by".isNumber() ? params.by as int : 0
    }

    void write(Record record) {
        batchList << record
        if (batchList.size() >= sizeBatch) {
            batchList.each { r ->
                writer.write(r)
            }
            batchList.clear()
        }
    }

    void close() {
        batchList.each { r ->
            writer.write(r)
        }
        writer.close()
    }
}
