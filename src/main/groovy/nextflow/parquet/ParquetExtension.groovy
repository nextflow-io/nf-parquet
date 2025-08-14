package nextflow.parquet


import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint

import com.jerolba.carpet.CarpetReader
import com.jerolba.carpet.CarpetWriter

/**
 * Implements extensions for reading and writing Parquet files.
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 * @author Jorge Aguilera <jorge@edn.es>
 */
@Slf4j
@CompileStatic
class ParquetExtension extends PluginExtensionPoint {

    private Session session

    @Override
    protected void init(Session session) {
        this.session = session
    }

    /**
     * Load a Parquet file emitting each row as a separate item.
     *
     * @param path
     */
    @Operator
    DataflowWriteChannel splitParquet(DataflowReadChannel source, Map params=[:]) {
        final target = CH.create()
        final splitter = new ParquetSplitter(target, params)

        final onNext = { it -> splitter.apply(it) }
        final onComplete = { target << Channel.STOP }
        DataflowHelper.subscribeImpl(source, [onNext: onNext, onComplete: onComplete])

        return target
    }

    /**
     * Write each item in a source channel to a Parquet file.
     *
     * @param source
     * @param path
     */
    @Operator
    DataflowWriteChannel toParquet(DataflowReadChannel source, String path, Map params=[:]) {
        final target = CH.createBy(source)
        final writer = new ParquetWriter(path, params)
        final onNext = {
            writer.write(it as Record)
            target << it
        }
        final onComplete = {
            writer.close()
            target << Channel.STOP
        }
        DataflowHelper.subscribeImpl(source, [onNext: onNext, onComplete: onComplete])
        return target
    }

    class ParquetSplitter {
        private DataflowWriteChannel target
        private Class<Record> clazz

        ParquetSplitter(DataflowWriteChannel target, Map params) {
            this.target = target
            if( params.record ) {
                if (!(params.record instanceof Class<Record>)) {
                    throw new IllegalArgumentException("A Record.class is required. Class provided $params.record")
                }
                this.clazz = params.record as Class<Record>
            }
        }

        void apply(Object source) {
            try {
                log.debug "Start reading $source, with projection ${clazz ?: 'raw'}"
                // create parquet reader
                final reader = new CarpetReader(toFile(source), clazz ?: Map)
                for (def record : reader) {
                    target << record
                }
            }
            catch( IOException e ) {
                throw new IllegalStateException("Error while reading Parquet file - cause: ${e.message ?: e}", e)
            }
        }

        private File toFile(Object source){
            return switch( source ){
                case {it instanceof String}->Path.of(source as String).toFile()
                case {it instanceof Path} -> (source as Path).toFile()
                default->throw new IllegalArgumentException("Invalid input for splitParquet operator: ${source}")
            }
        }

    }

    class ParquetWriter implements Closeable{
        private Class<Record> clazz
        CarpetWriter writer
        ParquetWriter(String output, Map params){
            if( !params.record ||!(params.record instanceof Class<Record>)) {
                throw new IllegalArgumentException("A Record.class is required. Class provided $params.record")
            }
            this.clazz = params.record as Class<Record>
            var outputStream = new FileOutputStream(output)
            writer = new CarpetWriter<>(outputStream, this.clazz)
        }

        void write(Record record){
            writer.write(record)
        }

        void close(){
            writer.close()
        }
    }
}
