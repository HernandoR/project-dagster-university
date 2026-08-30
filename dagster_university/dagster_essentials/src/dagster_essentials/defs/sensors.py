# src/dagster_essentials/defs/sensors.py
import dagster as dg
import os
import json
import pathlib
from rootutils import autosetup
from dagster_essentials.defs.jobs import adhoc_request_job

@dg.sensor
def adhoc_request_sensor(context: dg.SensorEvaluationContext):
    PATH_TO_REQUESTS= 