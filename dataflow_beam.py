import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows

# Define your pipeline options
options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

# Define your data processing pipeline
def run():
    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription='projects/my-baby-project-422109/subscriptions/demotopic-sub')
            | 'Window into Fixed Intervals' >> beam.WindowInto(FixedWindows(60))
            | 'Parse JSON' >> beam.Map(lambda x: json.loads(x.decode('utf-8')))
            | 'Transform Data' >> beam.Map(lambda record: {
                'PlateNumb': record.get('PlateNumb'),
                'RouteName': record.get('RouteName', {}).get('Zh_tw'),
                'Direction': record.get('Direction'),
                'StopName': record.get('StopName', {}).get('Zh_tw'),
                'StopSequence': record.get('StopSequence'),
                'GPSTime': record.get('GPSTime')
            })
        )

        # Write to BigQuery
        messages | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            'my-baby-project-422109:pubsub_bus.pubsub_bus_299',
            schema='PlateNumb:STRING, RouteName:STRING, Direction:INTEGER, StopName:STRING, StopSequence:INTEGER, GPSTime:TIMESTAMP',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    run()
