import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input',
            help='Path of the file to read from')
        parser.add_argument(
            '--output',
            required=True,
            help='Output file to write results to.')

with beam.Pipeline(options=MyOptions()) as p:
    table_schema = 'PassengerId:INTEGER,Survived:INTEGER'

    lines = (p  | 'read' >> ReadFromText(p.options.input)
                | 'Write Data to BigQuery' >> WriteToBigQuery(
                        p.options.output,
                        schema=table_schema,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE))

