from aws_cdk import (
    # Duration,
    Stack, Duration,
    # aws_sqs as sqs,
)
from aws_cdk.aws_cloudwatch import Alarm
from aws_cdk.aws_cloudwatch_actions import SnsAction
from aws_cdk.aws_dynamodb import Table, AttributeType
from aws_cdk.aws_events import Rule, Schedule
from aws_cdk.aws_events_targets import LambdaFunction
from aws_cdk.aws_lambda import Runtime
from aws_cdk.aws_lambda_python_alpha import PythonFunction
from aws_cdk.aws_secretsmanager import Secret
from aws_cdk.aws_sns import Topic
from aws_cdk.aws_sns_subscriptions import EmailSubscription
from constructs import Construct


class ParteispendenStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        was_ist_schon_getan = Table(self, 'was_ist_schon_getan', partition_key={'name': 'id', 'type': AttributeType.STRING})
        # Das Secret aus der ARN importieren
        secret = Secret(self, 'bluesky-login')

        bundestag_scrapen = PythonFunction(
            self, 'bundestag_scrapen',
            entry='bundestag_scrapen',
            runtime=Runtime.PYTHON_3_12,
            timeout=Duration.seconds(30),
            environment={
                'WAS_IST_GETAN_TABELLE': was_ist_schon_getan.table_name,
                "BSKY_LOGIN": secret.secret_arn
            }
        )
        secret.grant_read(bundestag_scrapen)

        was_ist_schon_getan.grant_read_write_data(bundestag_scrapen)

        event_rule = Rule(
            self,
            "ScheduleRule",
            schedule=Schedule.rate(Duration.hours(1))
        )

        # Add the Lambda function as the target of the rule
        event_rule.add_target(LambdaFunction(bundestag_scrapen))

        error_topic = Topic(
            self, "LambdaErrorTopic",
            display_name="Lambda Fehler-Benachrichtigungen"
        )

        # E-Mail-Adresse abonnieren
        error_topic.add_subscription(EmailSubscription("sebastian.annies@gmail.com"))

        # CloudWatch-Alarm für Lambda-Fehler
        error_alarm = Alarm(
            self, "LambdaErrorAlarm",
            metric=bundestag_scrapen.metric_errors(),
            threshold=1,  # Fehler-Schwelle
            evaluation_periods=1,  # Anzahl der fehlerhaften Zeiträume, bevor Alarm ausgelöst wird
            alarm_description="Alarm bei Fehlern in der Lambda-Funktion",
            actions_enabled=True
        )

        # SNS-Thema mit dem Alarm verknüpfen
        error_alarm.add_alarm_action(
            SnsAction(error_topic)
        )