import requests
import json
import time
import uuid
from pathlib import Path
import logging

# --- Agent API Configuration ---
api_url = 'https://backend.ren3.ai'
user_uuid = "89ce6ee3-9428-49ec-bbda-70ccd6ab0dd2"
workspace_id = "67c9c8b1-060d-4831-9d2f-981bf9f4e4b3"
agent_uuid = 'bc996a6e-74c7-4e78-a610-d9c75d7cdff3'
agent_folder = '0637197c-f133-4ade-9118-772cc07d5bf8'
poll_interval = 15
max_retries = 2

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s'
)
logger = logging.getLogger(__name__)


def send_csv_to_agent(csv_file_path):
    """
    Uploads CSV file to agent, runs it, and waits for completion.
    No output download - just confirms job is done.
    
    Args:
        csv_file_path: Path to CSV file
        
    Returns:
        bool: True if successful, False if failed
    """
    csv_file_path = Path(csv_file_path) if isinstance(csv_file_path, str) else csv_file_path
    
    if not csv_file_path.exists():
        logger.error(f"CSV file not found: {csv_file_path}")
        return False
    
    # Generate temp folder UUID for this upload
    temp_folder_uuid = str(uuid.uuid4())
    
    # --- Retry Loop ---
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                logger.info(f"Retrying (Attempt {attempt + 1}/{max_retries + 1})")
            else:
                logger.info(f"Uploading CSV to agent: {csv_file_path.name}")
            
            # --- 1. Upload CSV File ---
            upload_url = f"{api_url}/upload_agenttmpfiles"
            
            with open(csv_file_path, 'rb') as f:
                files = [('file', (csv_file_path.name, f, 'text/csv'))]
                
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
            
            logger.info(f"✓ CSV uploaded. Waiting for ingestion...")
            time.sleep(5)
            
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
            
            logger.info(f"File verified")
            
            # --- 3. Run Agent ---
            logger.info(f"Running agent...")
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
            
            logger.info(f"Agent job started: {job_id}")
            
            # --- 4. Poll Job Status (Wait for Completion) ---
            logger.info(f"Waiting for agent to complete...")
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
                    logger.info(f"Agent completed successfully!")
                    return True
                elif is_failed:
                    raise Exception(f"Agent job failed - check logs")
                else:
                    logger.info(f"  Processing... ({poll_count + 1}/{max_polls})")
                    time.sleep(poll_interval)
            
            # Timeout
            raise Exception(f"Agent job timed out after {max_polls * poll_interval} seconds")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API error (Attempt {attempt + 1}): {e}")
            
            if attempt == max_retries:
                logger.error(f"All {max_retries + 1} attempts failed.")
                return False
            
            # Exponential backoff
            delay = 2 * (2 ** attempt)
            logger.info(f"Waiting {delay} seconds before retrying...")
            time.sleep(delay)
        
        except Exception as e:
            logger.error(f"Error: {e}")
            
            if attempt == max_retries:
                logger.error(f"All {max_retries + 1} attempts failed.")
                return False
            
            delay = 2 * (2 ** attempt)
            logger.info(f"Waiting {delay} seconds before retrying...")
            time.sleep(delay)
    
    return False


def process_all_csvs_in_folder(folder_path):
    """
    Process all CSV files in the given folder.
    
    Args:
        folder_path: Path to folder containing CSV files
        
    Returns:
        dict: Summary of processing results
    """
    folder = Path(folder_path)
    
    if not folder.exists():
        logger.error(f"Folder not found: {folder_path}")
        return {'success': False, 'error': 'Folder not found'}
    
    # Get all CSV files
    csv_files = sorted([f for f in folder.iterdir() if f.is_file()])
    
    if not csv_files:
        logger.warning(f"No CSV files found in {folder_path}")
        return {'success': False, 'error': 'No CSV files found'}
    
    logger.info(f"Found {len(csv_files)} CSV file(s) to process")
    
    # Track results
    results = {
        'total': len(csv_files),
        'success': 0,
        'failed': 0,
        'files': []
    }
    
    # Process each CSV
    for idx, csv_file in enumerate(csv_files, 1):
        logger.info(f"\n{'=' * 60}")
        logger.info(f"FILE {idx}/{len(csv_files)}: {csv_file.name}")
        logger.info(f"{'=' * 60}")
        
        success = send_csv_to_agent(csv_file)
        
        if success:
            results['success'] += 1
            results['files'].append({
                'file': csv_file.name,
                'status': 'success'
            })
            logger.info(f"{csv_file.name} processed successfully")
        else:
            results['failed'] += 1
            results['files'].append({
                'file': csv_file.name,
                'status': 'failed'
            })
            logger.error(f"{csv_file.name} failed")
        
        # Wait 2 seconds before next file (except for last one)
        if idx < len(csv_files):
            logger.info(f"Pausing 2 seconds before next file...")
            time.sleep(2)
    
    return results


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("CSV TO AGENT BATCH PROCESSOR")
    logger.info("=" * 60)
    
    # Process all CSVs in /app/final_output
    csv_folder = Path('/app/final_output')
    
    results = process_all_csvs_in_folder(csv_folder)
    
    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("PROCESSING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total files: {results.get('total', 0)}")
    logger.info(f"Successful: {results.get('success', 0)}")
    logger.info(f"Failed: {results.get('failed', 0)}")
    
    if results.get('files'):
        logger.info("\nDetailed results:")
        for file_result in results['files']:
            status_icon = "✓" if file_result['status'] == 'success' else "✗"
            logger.info(f"  {status_icon} {file_result['file']}: {file_result['status']}")
    
    logger.info("=" * 60)
    
    # Exit with appropriate code
    if results.get('success', 0) == results.get('total', 0):
        logger.info("ALL FILES PROCESSED SUCCESSFULLY!")
        exit(0)
    elif results.get('success', 0) > 0:
        logger.warning("PARTIAL SUCCESS - Some files failed")
        exit(1)
    else:
        logger.error("ALL FILES FAILED")
        exit(1)