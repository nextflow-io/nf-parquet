package nextflow.splitter

import com.google.common.hash.HashCode
import groovy.transform.*
import groovy.util.logging.Slf4j
import nextflow.parquet.impl.ParquetWriter

import java.nio.file.Path

@Slf4j
@CompileStatic
class ParquetFileCollector implements CollectorStrategy, CacheableCollector, Closeable{

    @ToString
    @EqualsAndHashCode
    @TupleConstructor
    @PackageScope
    static class CachePath {
        Path path
        HashCode hash
    }

    private ParquetWriter writer
    private Class clazz
    private Path currentPath
    private int index
    private int count

    ParquetFileCollector(CachePath base, Class clazz){
        assert base
        assert base.path

        this.baseFile = base.path
        this.hashCode = base.hash
        this.clazz = clazz
    }



    @Override
    void close() throws IOException {
        closeWriter()
    }

    @Override
    void add(Object record) {
        if( record == null || !(record instanceof Record || record instanceof Map) )
            return

        if( !writer ) {
            currentPath = getNextNameFor(baseFile, ++index)
            allPaths << currentPath
            writer = getOutputWriter(currentPath)
        }

        writer.write(record as Record)
    }

    @Override
    boolean hasChunk() {
        return currentPath != null
    }

    @Override
    Object nextChunk() {
        closeWriter()
        count = 0
        def result = currentPath
        currentPath = null
        return result
    }

    protected ParquetWriter getOutputWriter(Path path) {
        return new ParquetWriter(path.toAbsolutePath().toString(), [record:clazz])
    }

    Path getNextNameFor(Path file, int index) {
        def baseName = file.getBaseName()
        def suffix = file.getExtension()
        String fileName = suffix ? "${baseName}.${index}.${suffix}" : "${baseName}.${index}"
        return file.resolveSibling( fileName )
    }


    private void closeWriter() {
        if( writer ) {
            writer.closeQuietly()
            writer = null
        }
    }
}
