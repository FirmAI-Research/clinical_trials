
import dlt
import requests
from datetime import datetime
import os

COLORS = {
    "red": "\033[91m",
    "green": "\033[92m",
    "yellow": "\033[93m",
    "blue": "\033[94m",
    "reset": "\033[0m"
}

def log_message(pipeline_name, message, color="reset"):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    color_code = COLORS.get(color, COLORS["reset"]) 
    print(f"\033[93m[{timestamp}]\033[0m \033[94m[{pipeline_name}]\033[0m {color_code}{message}{COLORS['reset']}")

@dlt.resource(write_disposition="merge", primary_key="nctId", name="raw_clinicaltrials_studies")
def fetch_clinical_trials(next_page_token=dlt.sources.incremental("nextPageToken", initial_value=None)):
    api_url = "https://clinicaltrials.gov/api/v2/studies"
    params = {"pageSize": 1000}  # Fetch up to 1000 records per page
    max_batch_size = 5000        # Maximum batch size to be yielded at once
    total_records = 0            # Counter for the total number of records fetched
    record_limit = 5000          # Stop fetching after reaching this limit
    record_buffer = []           # Buffer to store fetched records

    # Use the resource-scoped state for the next page token
    state = dlt.current.resource_state()
    current_page_token = state.get("nextPageToken", next_page_token.start_value)
    pipeline_name = dlt.current.pipeline().pipeline_name

    while True:
        if current_page_token:
            log_message(pipeline_name, f"Fetching data with page token: {current_page_token}", "green")
            params["pageToken"] = current_page_token
        else:
            log_message(pipeline_name, "Starting from the first page.", "green")

        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()
            data = response.json()

            log_message(pipeline_name, f"Request URL: {response.url}", "yellow")

            # Extract the next page token and studies
            next_page_token_value = data.get("nextPageToken")
            log_message(pipeline_name, f"Next page token to fetch: {next_page_token_value}", "green")

            # Collect studies - This was necessary once dlt expects for those to be withing the root dictionary!
            studies = [
                {
                    "nctId": study.get("protocolSection", {}).get("identificationModule", {}).get("nctId"),
                    "nextPageToken": next_page_token_value,
                    **study
                }
                for study in data.get("studies", [])
            ]

            record_buffer.extend(studies)
            total_records += len(studies)

            log_message(pipeline_name, f"Retrieved {len(studies)} records, Total so far: {total_records}", "green")

            if len(record_buffer) >= max_batch_size:
                log_message(pipeline_name, f"Saving batch of {len(record_buffer)} records to DuckDB", "blue")
                yield record_buffer
                record_buffer = []  # Clear the buffer after yielding

            # Update the next page token for the next iteration
            current_page_token = next_page_token_value
            state["nextPageToken"] = current_page_token

            if total_records >= record_limit:
                if record_buffer:
                    log_message(pipeline_name, f"Saving remaining {len(record_buffer)} records to DuckDB", "blue")
                    yield record_buffer
                log_message(pipeline_name, f"Reached the record limit of {record_limit}. Stopping data fetch.", "yellow")
                break

            if not next_page_token_value:
                if record_buffer:
                    log_message(pipeline_name, f"Saving remaining {len(record_buffer)} records to DuckDB", "blue")
                    yield record_buffer
                log_message(pipeline_name, "No more pages to fetch. Data retrieval complete.", "green")
                break

        except Exception as e:
            log_message(pipeline_name, f"Error fetching data: {e}", "red")
            break


@dlt.source
def clinical_trials_source():
    return fetch_clinical_trials()


if __name__ == "__main__":
    # Ensure the current directory is used for DLT state files
    dlt.config.state_directory = os.path.join(os.getcwd(), "dlt_state")

    pipeline = dlt.pipeline(
        pipeline_name="clinical_trials_pipeline",
        destination=dlt.destinations.duckdb(os.path.join(os.getcwd(), "clinical_trials.duckdb")),  # DuckDB file lives in in current dir
        dataset_name="raw_data",
    )


    log_message(pipeline.pipeline_name, "Starting pipeline execution...", "blue")
    try:
        info = pipeline.run(clinical_trials_source())
        log_message(pipeline.pipeline_name, f"Pipeline completed successfully. Records processed: {info}", "green")

    except Exception as e:
        log_message(pipeline.pipeline_name, f"Pipeline execution failed: {e}", "red")
