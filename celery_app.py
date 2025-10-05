from celery import Celery
from config import Config
import logging
import subprocess
import json
from operations import DatabaseOperations
from progress_tracker import ProgressTracker

logger = logging.getLogger(__name__)

app = Celery('celery_app', broker=Config.CELERY_BROKER_URL, backend=Config.CELERY_RESULT_BACKEND)

app.conf.update(
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour max
    task_soft_time_limit=3300,  # 55 minutes
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)

@app.task(bind=True, name='celery_app.crawl_task')
def crawl_task(self, crawl_info):
    """
    Celery task to run Scrapy spider using subprocess.Popen
    This avoids the 'daemonic processes are not allowed to have children' error
    """
    url = crawl_info.get('url')
    domain = crawl_info.get('domain')

    if not url or not domain:
        logger.error("Missing url or domain in crawl_info")
        DatabaseOperations.insert_log(domain or 'unknown', "Missing url or domain", "ERROR")
        return {'error': 'Missing url or domain', 'status': 'failed', 'progress': 0}

    try:
        logger.info(f"Starting crawl task for URL: {url}, Domain: {domain}")
        DatabaseOperations.insert_log(domain, f"Crawl task started for {url}", "INFO")

        # Initialize progress tracker
        tracker = ProgressTracker(domain)
        tracker.reset()
        tracker.set_total(100)

        # Prepare scrapy command
        cmd = [
            'scrapy', 'runspider', 'spider.py',
            '-a', f'start_url={url}',
            '-a', f'domain={domain}',
            '-s', 'ITEM_PIPELINES={"pipelines.PostgreSQLPipeline": 300}',
            '-s', 'LOG_LEVEL=INFO',
        ]

        logger.info(f"Executing command: {' '.join(cmd)}")
        DatabaseOperations.insert_log(domain, f"Executing spider command", "INFO")

        # Run subprocess with proper output handling
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )

        # Store process info for monitoring
        logger.info(f"Spider process started with PID: {process.pid}")

        # Monitor process output in real-time
        stdout_lines = []
        stderr_lines = []

        # Read stdout
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                if not line:
                    break
                stdout_lines.append(line.strip())
                logger.debug(f"Spider output: {line.strip()}")

                # Update task state periodically
                current_progress = tracker.get_percentage()
                self.update_state(
                    state='PROGRESS',
                    meta={
                        'progress': current_progress,
                        'domain': domain,
                        'status': 'running'
                    }
                )

        # Wait for process to complete
        return_code = process.wait(timeout=3000)  # 50 minutes timeout

        # Read stderr after completion
        if process.stderr:
            stderr_output = process.stderr.read()
            if stderr_output:
                stderr_lines.append(stderr_output)
                logger.warning(f"Spider stderr: {stderr_output[:500]}")

        # Check result
        if return_code == 0:
            final_progress = tracker.get_percentage()
            logger.info(f"Crawl completed successfully for {domain}. Progress: {final_progress}%")
            DatabaseOperations.insert_log(domain, f"Crawl completed successfully", "INFO")

            return {
                'status': 'completed',
                'progress': final_progress,
                'domain': domain,
                'message': 'Crawl completed successfully'
            }
        else:
            error_msg = '\n'.join(stderr_lines[-10:]) if stderr_lines else 'Unknown error'
            logger.error(f"Crawl failed for {domain}: Return code {return_code}")
            DatabaseOperations.insert_log(domain, f"Crawl failed with code {return_code}: {error_msg[:500]}", "ERROR")

            return {
                'status': 'failed',
                'progress': 0,
                'domain': domain,
                'error': error_msg[:1000],
                'return_code': return_code
            }

    except subprocess.TimeoutExpired:
        error_msg = "Spider execution timed out after 50 minutes"
        logger.error(f"{error_msg} for {domain}")
        DatabaseOperations.insert_log(domain, error_msg, "ERROR")

        # Kill the process
        if process:
            process.kill()

        return {
            'status': 'failed',
            'progress': 0,
            'domain': domain,
            'error': error_msg
        }

    except FileNotFoundError:
        error_msg = "Scrapy command not found. Make sure Scrapy is installed and in PATH"
        logger.error(error_msg)
        DatabaseOperations.insert_log(domain, error_msg, "ERROR")

        return {
            'status': 'failed',
            'progress': 0,
            'domain': domain,
            'error': error_msg
        }

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Crawl task failed unexpectedly: {error_msg}")
        DatabaseOperations.insert_log(domain, f"Task failed: {error_msg[:500]}", "ERROR")

        return {
            'status': 'failed',
            'progress': 0,
            'domain': domain,
            'error': error_msg
        }

    finally:
        # Cleanup
        if process and process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()
