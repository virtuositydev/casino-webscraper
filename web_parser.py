import pandas as pd
import requests
import json
import time
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
import logging

# --- Agent API Configuration ---
# api_url = 'https://api-solaire.ren3.ai'
# user_uuid = "96df854c-51bf-4126-a71a-e52872ccdedd"
# workspace_id = '3f9b8a90-7676-4458-a797-e83e45d1d7a5'
# agent_uuid = 'bcfb8374-f58e-42aa-8d90-592539d1a938'
# agent_folder = '130a4824-f8a2-41d8-9ca3-bb2e04b66ffe'
# poll_interval = 15
# max_retries = 2  

api_url = 'https://backend.ren3.ai'
user_uuid = "89ce6ee3-9428-49ec-bbda-70ccd6ab0dd2"
workspace_id = "67c9c8b1-060d-4831-9d2f-981bf9f4e4b3"
agent_uuid = '72bf3fc2-bab0-427c-bcc5-7b46be52d7b9'
agent_folder = '9df4348b-4779-4339-8837-a645ca8e4857'
poll_interval = 15
max_retries = 2  

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s'
)
logger = logging.getLogger(__name__)

def call_api(json_file_path, user_uuid, workspace_id):
    """
    Replaces the original call_api function with agent-based processing.
    Uploads single JSON file, runs agent, polls for completion, downloads result.
    
    Returns the parsed JSON answer object, maintaining compatibility with original flow.
    """
    
    # --- Retry Configuration (keeping their pattern) ---
    base_delay_seconds = 2  # Wait 2s, then 4s on subsequent failures
    
    # Generate temp folder UUID for this file
    temp_folder_uuid = str(uuid.uuid4())
    
    # --- Retry Loop (keeping their structure) ---
    for attempt in range(max_retries + 1):  # Loop 3 times (0, 1, 2)
        try:
            if attempt > 0:  # This is a retry
                print(f"Retrying (Attempt {attempt + 1}/{max_retries + 1}).")
            else:  # This is the first attempt
                print(f"Uploading file to agent...")
            
            # --- 1. Upload File ---
            upload_url = f"{api_url}/upload_agenttmpfiles"
            
            with open(json_file_path, 'rb') as f:
                files = [('file', (json_file_path.name, f, 'application/json'))]
                
                form_data = {
                    'workspaceid': workspace_id,
                    'useruuid': user_uuid,
                    'uploadtype': 'agents',
                    'fileignoreparent': 'false',
                    'parentfolder': temp_folder_uuid,
                    'forceOverwrite': 'true',
                    'tempfolderuuid': temp_folder_uuid,
                    'agentuuid': agent_uuid,
                    'agent_folder': agent_folder,
                    'extra': json.dumps({
                        'tempfolderuuid': temp_folder_uuid,
                        'agentuuid': agent_uuid,
                        'agent_folder': agent_folder
                    })
                }
                
                response = requests.post(upload_url, data=form_data, files=files, timeout=300)
                response.raise_for_status()
                upload_result = response.json()
                
                if not upload_result.get('success'):
                    raise Exception(f"Upload failed: {upload_result}")
            
            print(f"File uploaded successfully. Waiting for ingestion...")
            time.sleep(5)  # Wait for file ingestion
            
            # --- 2. Get Input Files ---
            input_files_url = f"{api_url}/agentdrive/get_jobinputfiles"
            input_files_data = {
                'input_folder': temp_folder_uuid,
                'userid': user_uuid,
                'workspaceid': workspace_id
            }
            
            response = requests.post(input_files_url, json=input_files_data, timeout=60)
            response.raise_for_status()
            files_result = response.json()
            
            if not files_result.get('success'):
                raise Exception(f"Failed to get input files: {files_result}")
            
            input_files = files_result.get('returnObject', [])
            
            # Handle different response formats
            if isinstance(input_files, dict):
                if 'files' in input_files:
                    input_files = input_files['files']
                else:
                    input_files = [input_files]
            
            if not input_files:
                raise Exception("No input files found after upload")
            
            # --- 3. Run Agent ---
            print(f"Running agent job...")
            run_agent_url = f"{api_url}/agentdrive/run_agent"
            
            run_data = {
                'data': {
                    'agent_uuid': agent_uuid,
                    'input_files': input_files,
                    'temp_folder': temp_folder_uuid
                },
                'userid': user_uuid,
                'workspaceid': workspace_id
            }
            
            response = requests.post(run_agent_url, json=run_data, timeout=60)
            response.raise_for_status()
            run_result = response.json()
            
            if not run_result.get('success'):
                raise Exception(f"Failed to run agent: {run_result}")
            
            job_id = run_result.get('returnObject', {}).get('uuid')
            if not job_id:
                raise Exception("No job ID returned from agent")
            
            print(f"Agent job started: {job_id}")
            
            # --- 4. Poll Job Status ---
            print(f"Waiting for agent to complete...")
            status_url = f"{api_url}/agentdrive/get_agentjoblogs"
            max_polls = 40  # 40 * 15 seconds = 10 minutes max
            
            for poll_count in range(max_polls):
                status_data = {
                    'uuid': job_id,
                    'userid': user_uuid,
                    'workspaceid': workspace_id
                }
                
                response = requests.post(status_url, json=status_data, timeout=60)
                response.raise_for_status()
                status_result = response.json()
                
                if not status_result.get('success'):
                    raise Exception(f"Failed to get job status: {status_result}")
                
                # Check logs for completion
                logs = status_result.get('returnObject', [])
                is_completed = False
                is_failed = False
                
                for log_entry in logs:
                    log_text = log_entry.get('text', '').lower()
                    log_type = log_entry.get('type', 0)
                    
                    # Type 2 indicates completion
                    if log_type == 2 and 'completed' in log_text:
                        is_completed = True
                        break
                    elif 'failed' in log_text or 'error' in log_text:
                        is_failed = True
                        break
                
                if is_completed:
                    print(f"Agent job completed successfully")
                    break
                elif is_failed:
                    raise Exception(f"Agent job failed - check logs")
                else:
                    print(f"  Processing... (poll {poll_count + 1}/{max_polls})")
                    time.sleep(poll_interval)
            else:
                raise Exception(f"Agent job timed out after {max_polls * poll_interval} seconds")
            
            # --- 5. Get Job Details ---
            details_url = f"{api_url}/agentdrive/get_jobdetails"
            details_data = {
                'detailed': 1,
                'uuid': job_id,
                'userid': user_uuid,
                'workspaceid': workspace_id
            }
            
            response = requests.post(details_url, json=details_data, timeout=60)
            response.raise_for_status()
            details_result = response.json()
            
            if not details_result.get('success'):
                raise Exception(f"Failed to get job details: {details_result}")
            
            output_folder = details_result.get('returnObject', {}).get('agentJob', {}).get('output_folder')
            
            # --- 6. Get Output Files ---
            output_files_url = f"{api_url}/tensordrive/get_docs"
            output_data = {
                'type': 'agents',
                'fields': ['uuid', 'doc_filename', 'is_folder', 'doc_extension'],
                'filter': {
                    'status': ['', None],
                    'parent_folder': output_folder,
                    'workspace_id': workspace_id,
                    'ingestion_status': 5,
                    'folder_type': {'operator': 'ISNULLANDVALUE', 'value': 0},
                    'isbundlechild': {'operator': 'ISNULLANDVALUE', 'value': 0},
                    'latest_version': 1
                },
                'parent_folder': output_folder,
                'useruuid': user_uuid,
                'workspaceid': workspace_id,
                'order': 'is_folder DESC,folder_type ASC,doc_filename ASC'
            }
            
            response = requests.post(output_files_url, json=output_data, timeout=60)
            response.raise_for_status()
            output_result = response.json()
            
            if not output_result.get('success'):
                raise Exception(f"Failed to get output files: {output_result}")
            
            output_files = output_result.get('returnObject', [])
            
            # Find the JSON output file (assuming agent produces JSON now)
            json_output_file = None
            for file in output_files:
                # Adjust filename based on what your agent actually produces
                if file['doc_filename'].endswith('.json'):
                    json_output_file = file
                    break
            
            if not json_output_file:
                # If no JSON, maybe agent still produces CSV, try to handle that
                for file in output_files:
                    if file['doc_filename'].endswith('.csv'):
                        # Download CSV and convert to expected format
                        print("Warning: Agent produced CSV instead of JSON, attempting to parse...")
                        json_output_file = file
                        break
                
                if not json_output_file:
                    raise Exception("No output file found from agent")
            
            # --- 7. Download Output ---
            print(f"Downloading agent output...")
            download_url = f"{api_url}/tensordrive/get_filestream"
            download_data = {
                'docuuid': json_output_file['uuid'],
                'userid': user_uuid,
                'workspaceid': workspace_id
            }
            
            response = requests.post(download_url, json=download_data, timeout=120, stream=True)
            response.raise_for_status()
            
            # Parse the downloaded content
            content = response.content.decode('utf-8')
            
            # If it's JSON, parse directly
            if json_output_file['doc_filename'].endswith('.json'):
                try:
                    final_answer_object = json.loads(content)
                    return final_answer_object
                except json.JSONDecodeError:
                    return {"error": "Failed to decode agent output as JSON", "raw_content": content}
            
            # If it's CSV, convert to dict format (keeping their error format)
            elif json_output_file['doc_filename'].endswith('.csv'):
                try:
                    # Parse CSV and convert to expected format
                    import io
                    df = pd.read_csv(io.StringIO(content))
                    # Convert first row to dict (or adjust based on your needs)
                    if not df.empty:
                        return df.iloc[0].to_dict()
                    else:
                        return {"error": "Empty CSV returned from agent"}
                except Exception as e:
                    return {"error": f"Failed to parse CSV output: {e}", "raw_content": content}
            
        except requests.exceptions.RequestException as e:
            # --- FAILURE (keeping their error handling pattern) ---
            print(f"API error (Attempt {attempt + 1}): {e}")
            
            if attempt == max_retries:
                # This was the last attempt, so give up and return an error
                print(f"All {max_retries + 1} attempts failed. Moving to next row.")
                return {"error": f"API_ERROR: All retries failed. Last error: {e}"}
            
            # Not the last attempt, wait and retry (exponential backoff)
            delay = base_delay_seconds * (2 ** attempt)  # 2s, then 4s
            print(f"Waiting {delay} seconds before retrying.")
            time.sleep(delay)
    
    # This line should not be reachable
    return {"error": "API_ERROR: Unknown error in call_agent_api."}

def process_file_and_save_csv(promo_path, user_uuid, workspace_id, output_csv_path):
    """
    Reads a json file, gets the parsed JSON answer object from the API,
    and saves all answer objects into a single CSV file.
    
    Includes a 'finally' block to save partial results in case of a crash.
    """
    all_answer_objects = []
    
    try:
        # --- 1. FILE LOADING ---
        promo_folder = Path(promo_path)
        
        # Check if folder exists
        if not promo_folder.exists():
            print(f"Error: The folder at {promo_path} does not exist.")
            return False
        
        json_files = []
        for file in promo_folder.glob("*.json"):
            # Skip special files (starting with underscore)
            if file.name.startswith('_'):
                continue
            json_files.append(file)
        
        # Sort by filename for consistent ordering
        json_files.sort()
        
        if not json_files:
            print(f"No JSON files found in {promo_path}")
            return False
        
        print(f"Found {len(json_files)} JSON files to process")

        # --- 2. API LOOP ---
        for idx, json_file in enumerate(json_files, 1):

            print(f"\nProcessing file {idx}/{len(json_files)}: {json_file.name}")
            print(f"Time: {datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')}")
            
            answer_obj = call_api(json_file, user_uuid, workspace_id)
            
            if 'error' not in answer_obj:
                answer_obj['source_file'] = json_file.name
            
            all_answer_objects.append(answer_obj)
            
            # Add 20 second delay between files (except for last file)
            if idx < len(json_files):
                print(f"Waiting 20 seconds before next file...")
                time.sleep(20)

        print("\nProcessing complete. Saving all results.")
        return True # Signal success

    except json.JSONDecodeError as e:
        print(f"Error: Skipping {json_file.name} - Invalid JSON format - {e}")
    except KeyError as e:
        print(f"Error: Skipping {json_file.name} - Missing expected field - {e}")
    except FileNotFoundError:
        print(f"Error: The file at {promo_path} was not found.")
        return False
    except (KeyboardInterrupt, Exception) as e:
        # Catch errors or Ctrl+C
        print(f"\n\n--- PROCESS INTERRUPTED ---")
        print(f"Error/Interrupt: {e}")
        print("Attempting to save partial results.")
        return False
        
    finally:
        if len(all_answer_objects) > 0:
            print(f"\n--- SAVING {len(all_answer_objects)} RESULTS ---")
            
            try:
                # Convert the list of dictionaries into a pandas DataFrame.
                results_df = pd.DataFrame(all_answer_objects)
                
                # Save the DataFrame to a CSV file.
                results_df.to_csv(output_csv_path, index=False)
                
                print(f"Successfully saved results to '{output_csv_path}'")
            except Exception as save_e:
                print(f"FAILED TO SAVE RESULTS TO CSV")
                print(f"Save Error: {save_e}")
                # As a last resort, dump to an emergency JSON file
                emergency_file = output_csv_path + "_partial.json"
                print(f"Attempting to save as partial result to JSON file: {emergency_file}")
                try:
                    with open(emergency_file, 'w') as f:
                        json.dump(all_answer_objects, f, indent=4)
                    print(f"Partial data saved to '{emergency_file}'")
                except Exception as json_e:
                    print(f"FAILED TO SAVE EMERGENCY JSON. DATA IS LOST. Error: {json_e}")
        else:
            print("\nNo results to save.")

def get_latest_promo_folder():
    """Get the most recent promo folder"""
    output_dir = Path('/app/output')
    
    promo_folders = list(output_dir.glob('promo_*'))
    
    if not promo_folders:
        logger.warning("No promo folders found")
        return None
    
    # Sort by modification time (most recent first)
    promo_folders.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    
    latest = promo_folders[0]
    logger.info(f"Found latest promo folder: {latest.name}")
    
    return latest

if __name__ == "__main__":
    # Ikhwan - 2025-10-24
    # --- Configuration ---

    # Get promo folder path from command line argument
    promo_folder = get_latest_promo_folder()
    if not promo_folder:
        logger.error("No promo folders found in /app/output")
        exit(1)
    
    # Create processed directory for output
    processed_dir = Path('/app/processed') / promo_folder.name
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Output file in processed directory
    output_csv_file = processed_dir / 'web_promo_agent.csv'

    # --- Execution ---
    process_file_and_save_csv(
        str(promo_folder), 
        user_uuid, 
        workspace_id, 
        str(output_csv_file)
    )