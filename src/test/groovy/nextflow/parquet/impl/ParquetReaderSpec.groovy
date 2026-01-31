package nextflow.parquet.impl

import nextflow.parquet.DemoRecord
import spock.lang.Specification

class ParquetReaderSpec extends Specification{

    RecordConsumer newAware(List content) {
        new RecordConsumer() {
            @Override
            boolean wantMore(Object row) {
                content.add(row)
                false
            }
        }
    }

    def 'should validate parameters'(){
        when:
        new ParquetReader(null, [:])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetReader(newAware(), [illegalParameter: 1])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetReader(newAware(), [by: 1])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetReader(newAware(), [record: DemoRecord])
        then:
        true
    }


    def 'should read a parquet file'(){
        given:
        def path = getClass().getResource('/test.parquet').toURI().path
        def content = []
        def reader = new ParquetReader(newAware(content), [record: DemoRecord])
        when:
        reader.readFile( new File(path) )

        then:
        content.size() == 1
        content[0] instanceof  DemoRecord
    }

    def 'should read a raw parquet file'(){
        given:
        def path = getClass().getResource('/test.parquet').toURI().path
        def content = []
        def reader = new ParquetReader(newAware(content), [:])
        when:
        reader.readFile( new File(path) )

        then:
        content.size() == 1
        content[0] instanceof  Map
    }

}
