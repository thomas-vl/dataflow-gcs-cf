import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition

def run(argv=None):
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
        table_schema = 'PassengerId:INTEGER,Survived:INTEGER'
        print(known_args.input)
        lines = (p  | 'read' >> ReadFromText(known_args.input)
                    | 'Write Data to BigQuery' >> WriteToBigQuery(
                            known_args.output,
                            schema=table_schema,
                            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=BigQueryDisposition.WRITE_TRUNCATE))

        result = p.run()
        result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
