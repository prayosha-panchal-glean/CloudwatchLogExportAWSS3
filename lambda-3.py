import boto3
import json
import logging
from datetime import datetime, timedelta

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CloudWatchLogExporter:
    def __init__(self, log_group_name, destination_bucket, region):
        self.logs_client = boto3.client('logs', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)

        self.log_group_name = log_group_name
        self.destination_bucket = destination_bucket
        self.region = region

        # Each log group has a separate timestamp file inside "timestamps_glean_logforwarder/"
        self.s3_key = f"timestamps_glean_logforwarder/{self.log_group_name.replace('/', '-')}.json"

        logger.info(f"Initializing CloudWatchLogExporter for log group: {self.log_group_name} in region: {self.region}")
        logger.info(f"Using destination bucket: {self.destination_bucket}")
        logger.info(f"Timestamp file will be stored at: s3://{self.destination_bucket}/{self.s3_key}")

        self.last_export_time = self.get_last_export_time()
        self.current_time = int(datetime.utcnow().timestamp() * 1000)

    def get_last_export_time(self):
        """ Fetch the last export timestamp from S3 for the specific log group. """
        try:
            response = self.s3_client.get_object(Bucket=self.destination_bucket, Key=self.s3_key)
            data = json.loads(response['Body'].read().decode('utf-8'))
            last_export_time = int(data.get("last_export_time", self.default_last_export_time()))
            logger.info(f"Last export timestamp retrieved for {self.log_group_name}: {last_export_time}")
            return last_export_time
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"No timestamp file found for {self.log_group_name}. Exporting all available logs.")
            return self.get_log_group_creation_time()
        except Exception as e:
            logger.error(f"Error retrieving last export time for {self.log_group_name}: {str(e)}")
            return self.default_last_export_time()

    def get_log_group_creation_time(self):
        """ Get the creation time of the log group to export all available logs. """
        try:
            response = self.logs_client.describe_log_groups(logGroupNamePrefix=self.log_group_name)
            for log_group in response.get('logGroups', []):
                if log_group['logGroupName'] == self.log_group_name:
                    creation_time = int(log_group['creationTime'])
                    logger.info(f"Using log group creation time as start time: {creation_time}")
                    return creation_time
        except Exception as e:
            logger.error(f"Error retrieving log group creation time: {str(e)}")
        return self.default_last_export_time()

    def default_last_export_time(self):
        """ Default to exporting logs from the last 24 hours if no timestamp is found. """
        return int((datetime.utcnow() - timedelta(hours=24)).timestamp() * 1000)

    def has_new_logs(self):
        """ Check if new logs exist in the log group """
        try:
            response = self.logs_client.describe_log_streams(
                logGroupName=self.log_group_name,
                orderBy="LastEventTime",
                descending=True,
                limit=1
            )

            if 'logStreams' in response and response['logStreams']:
                latest_event_time = response['logStreams'][0].get('lastEventTimestamp', 0)
                logger.info(f"Latest event timestamp in {self.log_group_name}: {latest_event_time}, Last exported: {self.last_export_time}")

                return latest_event_time > self.last_export_time
        except Exception as e:
            logger.error(f"Error checking for new logs in {self.log_group_name}: {str(e)}")

        return False

    def update_last_export_time(self):
        """ Store the latest timestamp back to S3 for the specific log group with bucket owner full control. """
        try:
            self.s3_client.put_object(
                Bucket=self.destination_bucket,
                Key=self.s3_key,
                Body=json.dumps({"last_export_time": self.current_time}),
                ContentType='application/json',
                ACL='bucket-owner-full-control'
            )
            logger.info(f"Updated last export time for {self.log_group_name} to: {self.current_time}")
        except Exception as e:
            logger.error(f"Error updating last export time for {self.log_group_name}: {str(e)}")

    def create_export_task(self):
        """ Creates an export task from CloudWatch Logs to S3 only if new logs exist. """
        if not self.has_new_logs():
            logger.info(f"No new logs detected for {self.log_group_name}. Skipping export task creation.")
            return None

        try:
            response = self.logs_client.create_export_task(
                taskName=f"ExportTask-{self.log_group_name.replace('/', '-')}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
                logGroupName=self.log_group_name,
                fromTime=self.last_export_time,
                to=self.current_time - 1,  # Avoid future logs overlap
                destination=self.destination_bucket,
                destinationPrefix=f"logs/{self.log_group_name.replace('/', '-')}/{datetime.utcfromtimestamp(self.last_export_time / 1000).strftime('%Y%m%d-%H%M%S')}"
            )
            logger.info(f"Export task created successfully for {self.log_group_name} with Task ID: {response['taskId']}")

            # Update last export time only if the task is successfully created
            self.update_last_export_time()

            return response['taskId']
        except Exception as e:
            logger.error(f"Error creating export task for {self.log_group_name}: {str(e)}")
            return None

def lambda_handler(event, context):
    """ AWS Lambda handler function. """
    try:
        logger.info("Received event: " + json.dumps(event))

        log_group_name = event.get('LOG_GROUP_NAME')
        destination_bucket = event.get('DESTINATION_BUCKET')
        region = event.get('REGION')

        if not all([log_group_name, destination_bucket, region]):
            raise ValueError("Missing required parameters: LOG_GROUP_NAME, DESTINATION_BUCKET, REGION")

        exporter = CloudWatchLogExporter(log_group_name, destination_bucket, region)
        task_id = exporter.create_export_task()

        if task_id:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'Log export task created successfully for {log_group_name}',
                    'taskId': task_id,
                    'from': exporter.last_export_time,
                    'to': exporter.current_time
                })
            }
        else:
            return {
                'statusCode': 204,
                'body': json.dumps({'message': f'No new logs detected for {log_group_name}. Skipping task creation.'})
            }

    except Exception as e:
        logger.error(f"Lambda function error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Lambda execution error', 'error': str(e)})
        }