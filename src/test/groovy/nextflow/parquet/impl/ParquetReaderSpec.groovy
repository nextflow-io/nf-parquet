package nextflow.parquet.impl

import nextflow.parquet.DemoRecord
import spock.lang.Specification

class ParquetReaderSpec extends Specification{


    def 'should validate parameters'(){
        given:
        def aware = new ReadRecordAware() {
            @Override
            void recordRead(Object row) {
            }
        }

        when:
        new ParquetReader(null, [:])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetReader(aware, [illegalParameter: 1])
        then:
        thrown(IllegalArgumentException)

        when:
        new ParquetReader(aware, [by: 1])
        then:
        true

        when:
        new ParquetReader(aware, [record: DemoRecord])
        then:
        true
    }


    def 'should read a parquet file'(){
        given:
        def path = getClass().getResource('/test.parquet').toURI().path
        def content = []
        def aware = new ReadRecordAware() {
            @Override
            void recordRead(Object row) {
                content.add(row)
            }
        }
        def reader = new ParquetReader(aware, [record: DemoRecord])
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
        def aware = new ReadRecordAware() {
            @Override
            void recordRead(Object row) {
                content.add(row)
            }
        }
        def reader = new ParquetReader(aware, [:])
        when:
        reader.readFile( new File(path) )

        then:
        content.size() == 1
        content[0] instanceof  Map
    }

    def 'should read a parquet file in chunks'(){
        given:
        def path = getClass().getResource('/multiple.parquet').toURI().path
        def content = []
        def aware = new ReadRecordAware() {
            @Override
            void recordRead(Object row) {
                content.add(row)
            }
        }
        def reader = new ParquetReader(aware, [by:1, record: DemoRecord])
        when:
        reader.readFile( new File(path) )

        then:
        content.size() == 3
        content[0] instanceof Object[]
        content[0][0] instanceof DemoRecord
    }

}
