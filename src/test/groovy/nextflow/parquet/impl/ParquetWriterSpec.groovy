package nextflow.parquet.impl

import com.jerolba.carpet.CarpetReader
import nextflow.parquet.DemoRecord
import spock.lang.Specification

import java.nio.file.Files

class ParquetWriterSpec extends Specification{

    def 'should validate parameters'(){
        when:
        new ParquetWriter(null, [:])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetWriter("/tmp/test", [illegalParameter: 1])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetWriter("/tmp/test", [by: 1])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetWriter("/tmp/test", [record: DemoRecord])
        then:
        true

        when:
        new ParquetWriter("/tmp/test", [record: DemoRecord, by:1])
        then:
        true

        when:
        new ParquetWriter("/tmp/test", [record: DemoRecord, by:"1"])
        then:
        true
    }

    def 'should write records into a parquet file'(){
        given:
        def output = Files.createTempFile("", ".parquet")
        output.deleteOnExit()
        def writer = new ParquetWriter(output.toAbsolutePath().toString(), [record:DemoRecord] )

        when:
        writer.write(new DemoRecord(1L, "test") as Record)
        writer.close()

        then:
        output.size()

        when:
        def reader = new CarpetReader(output.toFile(), DemoRecord)
        then:
        reader.toList().size() == 1

    }

}
