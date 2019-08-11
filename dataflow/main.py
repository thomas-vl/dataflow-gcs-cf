import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import BigQuerySink
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.io.gcp.internal.clients import bigquery


class XyzOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--output',
            required=True,
            help='Output file to write results to.')
        parser.add_value_provider_argument(
            '--input',
            help='Input file to write results to.')

with beam.Pipeline(options=XyzOptions()) as p:

    class format_data(beam.DoFn):
        def process(self, element):
            return [{'PassengerId':element[0],'Survived':element[1]}]

    print(p.options.input)
    lines = (p  | 'read' >> ReadFromText(p.options.input)
                | 'format' >> beam.ParDo(format_data())
                | 'Write' >> beam.io.WriteToBigQuery(
                    p.options.output,
                    schema='PassengerId:INTEGER, Survived:INTEGER',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=100))

    result = p.run()
    result.wait_until_finish()

