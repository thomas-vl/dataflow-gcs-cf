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

parser = argparse.ArgumentParser()
parser.add_argument(
    '--output',
    required=True,
    help='Output file to write results to.')
parser.add_argument(
    '--input',
    help='Input file to write results to.')
known_args, pipeline_args = parser.parse_known_args(argv)
pipeline_options = PipelineOptions(pipeline_args)

with beam.Pipeline(options=pipeline_options) as p:

    class format_data(beam.DoFn):
        def process(self, element):
            return [{'PassengerId':element[0],'Survived':element[1]}]

    print(known_args.input)
    lines = (p  | 'read' >> ReadFromText(known_args.input)
                | 'format' >> beam.ParDo(format_data())
                | 'Write' >> beam.io.WriteToBigQuery(
                    known_args.output,
                    schema='PassengerId:INTEGER, Survived:INTEGER',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    batch_size=100))

    result = p.run()
    result.wait_until_finish()

