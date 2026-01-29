package nextflow.parquet

import nextflow.parquet.impl.ParquetReader
import nextflow.parquet.impl.ParquetWriter

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

import java.nio.file.Path


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
        final emitter = new EmitterRecordChannel(target)
        final reader = new ParquetReader(emitter, params)

        final onNext = { path -> reader.readFile(toFile(path)) }
        final onComplete = { emitter.complete() }

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

    private File toFile(Object source) {
        return switch (source) {
            case { it instanceof String } -> Path.of(source as String).toFile()
            case { it instanceof Path } -> (source as Path).toFile()
            default -> throw new IllegalArgumentException("Invalid input for splitParquet operator: ${source}")
        }
    }
}
