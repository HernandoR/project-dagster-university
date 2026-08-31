# src/dagster_essentials/defs/sensors.py
import dagster as dg
import os
import json
from rootutils import autosetup
from dagster_essentials.defs.jobs import adhoc_request_job


@dg.sensor(job=adhoc_request_job)
def adhoc_request_sensor(context: dg.SensorEvaluationContext):
    # PATH_TO_REQUESTS=
    project_root = autosetup(".env")
    path_to_requests = project_root / "data" / "requests"
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs_to_request = []

    request_files = list(path_to_requests.glob("*.json"))

    context.log.info(
        f"Found {len(request_files)} request files in {path_to_requests}, previous_state: {previous_state}"
    )

    for request_file in request_files:
        last_modified = request_file.stat().st_mtime
        filename = request_file.name
        current_state[request_file.name] = last_modified
        # if the file is new or has been modified since the last run, add it to the request queue
        if filename not in previous_state or previous_state[filename] != last_modified:
            # with open(file_path, "r") as f:
            #     request_config = json.load(f)
            request_config = json.loads(request_file.read_bytes())
            runs_to_request.append(
                dg.RunRequest(
                    run_key=f"adhoc_request_{filename}_{last_modified}",
                    run_config={
                        "ops": {
                            "adhoc_request": {
                                "config": {"filename": filename, **request_config}
                            }
                        }
                    },
                )
            )
        else:
            msg = (
                f"file: {request_file} is in previous_state {filename in previous_state}"
                + ""
                if filename not in previous_state
                else f" and modify time pre: {previous_state[filename]} current: {last_modified}, skipping"
            )
            context.log.info(msg)
    return dg.SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(current_state),
    )


# local debug
if __name__ == "__main__":
    from dagster_essentials.defs.jobs import adhoc_request_job

    sensor = adhoc_request_sensor
    context = dg.build_sensor_context(cursor=None)
    result = sensor(context)
    print(result)
