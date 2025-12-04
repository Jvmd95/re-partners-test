"""
E-commerce event processing pipeline.
"""
import argparse
import json
import logging
import os
from typing import Optional, Tuple

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
    GoogleCloudOptions,
)

from src.parsing import ParseAndRoute, Tags
from src.transforms import StripTimestampedValue, AddIngestionTimestamp, FormatForGCS, encode_for_pubsub

_schema_path = os.path.join(os.path.dirname(__file__), 'schemas.json')
with open(_schema_path, 'r') as f:
    _schemas = json.load(f)

ORDER_SCHEMA = {'fields': _schemas['orders']}
INVENTORY_SCHEMA = {'fields': _schemas['inventory_logs']}
ACTIVITY_SCHEMA = {'fields': _schemas['user_activity']}


def parse_arguments(argv: Optional[list] = None) -> Tuple[argparse.Namespace, list]:
    """Parse pipeline arguments."""
    parser = argparse.ArgumentParser(description='E-commerce Event Processing Pipeline')

    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    parser.add_argument('--input_subscription', required=True)
    parser.add_argument('--output_bucket', required=True)
    parser.add_argument('--bq_dataset', required=True)
    parser.add_argument('--dlq_topic', default=None)
    
    parser.add_argument('--is_backfill', type=str2bool, default=False)
    
    parser.add_argument('--s3_path', default='gs://historical-bucket/data/*.json')
    parser.add_argument('--window_size_seconds', type=int, default=60)

    return parser.parse_known_args(argv)


def build_pipeline(pipeline: beam.Pipeline, known_args: argparse.Namespace, project_id: str) -> None:
    """Build the event processing pipeline."""
    
    if known_args.is_backfill:
        raw_data = pipeline | 'ReadFromS3' >> beam.io.ReadFromText(known_args.s3_path)
    else:
        raw_data = pipeline | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            subscription=known_args.input_subscription
        )

    results = raw_data | 'ParseAndRoute' >> beam.ParDo(ParseAndRoute()).with_outputs(
        Tags.ORDER, Tags.INVENTORY, Tags.ACTIVITY, Tags.ERROR
    )

    window_size = known_args.window_size_seconds

    orders = (
        results[Tags.ORDER]
        | 'StripOrderTs' >> beam.ParDo(StripTimestampedValue())
        | 'AddOrderIngestion' >> beam.ParDo(AddIngestionTimestamp())
    )
    orders | 'WriteOrdersBQ' >> beam.io.WriteToBigQuery(
        table=f"{known_args.bq_dataset}.orders",
        schema=ORDER_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
    )
    (
        orders
        | 'WindowOrders' >> beam.WindowInto(window.FixedWindows(window_size))
        | 'FormatOrders' >> beam.ParDo(FormatForGCS('order'))
        | 'WriteOrdersGCS' >> beam.io.WriteToText(
            file_path_prefix=f"{known_args.output_bucket}/output/order/data",
            file_name_suffix='.jsonl'
        )
    )

    inventory = (
        results[Tags.INVENTORY]
        | 'StripInvTs' >> beam.ParDo(StripTimestampedValue())
        | 'AddInvIngestion' >> beam.ParDo(AddIngestionTimestamp())
    )
    inventory | 'WriteInvBQ' >> beam.io.WriteToBigQuery(
        table=f"{known_args.bq_dataset}.inventory_logs",
        schema=INVENTORY_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
    )
    (
        inventory
        | 'WindowInv' >> beam.WindowInto(window.FixedWindows(window_size))
        | 'FormatInv' >> beam.ParDo(FormatForGCS('inventory'))
        | 'WriteInvGCS' >> beam.io.WriteToText(
            file_path_prefix=f"{known_args.output_bucket}/output/inventory/data",
            file_name_suffix='.jsonl'
        )
    )

    activity = (
        results[Tags.ACTIVITY]
        | 'StripActTs' >> beam.ParDo(StripTimestampedValue())
        | 'AddActIngestion' >> beam.ParDo(AddIngestionTimestamp())
    )
    activity | 'WriteActBQ' >> beam.io.WriteToBigQuery(
        table=f"{known_args.bq_dataset}.user_activity",
        schema=ACTIVITY_SCHEMA,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS
    )
    (
        activity
        | 'WindowAct' >> beam.WindowInto(window.FixedWindows(window_size))
        | 'FormatAct' >> beam.ParDo(FormatForGCS('user_activity'))
        | 'WriteActGCS' >> beam.io.WriteToText(
            file_path_prefix=f"{known_args.output_bucket}/output/user_activity/data",
            file_name_suffix='.jsonl'
        )
    )

    dlq_topic = known_args.dlq_topic or f"projects/{project_id}/topics/backend-events-dlq"
    (
        results[Tags.ERROR]
        | 'StripErrorTs' >> beam.ParDo(StripTimestampedValue())
        | 'EncodeForPubSub' >> beam.Map(encode_for_pubsub)
        | 'WriteDLQ' >> beam.io.WriteToPubSub(topic=dlq_topic)
    )


def run(argv: Optional[list] = None) -> None:
    """Main entry point."""
    known_args, pipeline_args = parse_arguments(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    project_id = google_cloud_options.project
    if not project_id and ':' in known_args.bq_dataset:
        project_id = known_args.bq_dataset.split(':')[0]

    if not project_id:
        raise ValueError("Project ID required. Set --project or use project:dataset format.")

    if not known_args.is_backfill:
        pipeline_options.view_as(StandardOptions).streaming = True

    logging.info("Starting pipeline - Project: %s", project_id)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        build_pipeline(pipeline, known_args, project_id)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    run()
