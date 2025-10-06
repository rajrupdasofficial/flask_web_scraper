from flask import Flask, request, render_template, redirect, url_for, jsonify
import logging
import subprocess
import threading
import sys
import os
from datetime import datetime
from url_verifier import verify_url
from operations import DatabaseOperations
from progress_tracker import ProgressTracker
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
app = Flask(__name__, template_folder='templates')

Config.validate()
DatabaseOperations.initialize_database()

# Dictionary to store running crawl processes
active_crawls = {}


def run_crawl_task(url, domain):
    """Run Scrapy spider in subprocess with real-time output capture"""
    process = None
    try:
        logger.info(f"=" * 80)
        logger.info(f"Starting crawl task for URL: {url}, Domain: {domain}")
        logger.info(f"=" * 80)

        DatabaseOperations.insert_log(domain, f"Crawl task started for {url}", "INFO")

        # Initialize progress tracker
        tracker = ProgressTracker(domain)
        tracker.reset()
        tracker.set_total(100)

        # Update crawl statistics - mark as running
        DatabaseOperations.update_crawl_statistics(
            domain=domain,
            status='running',
            start_time=datetime.now()
        )

        # Get the current Python executable and script directory
        python_executable = sys.executable
        script_dir = os.path.dirname(os.path.abspath(__file__))
        spider_path = os.path.join(script_dir, 'spider.py')

        # Verify spider file exists
        if not os.path.exists(spider_path):
            raise FileNotFoundError(f"Spider file not found: {spider_path}")

        # Prepare scrapy command with full paths
        cmd = [
            python_executable, '-m', 'scrapy', 'runspider', spider_path,
            '-a', f'start_url={url}',
            '-a', f'domain={domain}',
            '-s', 'ITEM_PIPELINES={"pipelines.PostgreSQLPipeline": 300}',
            '-s', 'LOG_ENABLED=True',
            '-s', 'LOG_LEVEL=INFO',
            '-s', 'LOG_STDOUT=True',
        ]

        logger.info(f"Executing command: {' '.join(cmd)}")
        logger.info(f"Working directory: {script_dir}")
        DatabaseOperations.insert_log(domain, f"Executing spider command", "INFO")

        # Run subprocess with real-time output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Merge stderr with stdout
            text=True,
            bufsize=1,
            universal_newlines=True,
            cwd=script_dir,
            env=os.environ.copy()
        )

        # Store process for tracking
        active_crawls[domain] = process
        logger.info(f"Spider process started with PID: {process.pid}")

        # Monitor process output in real-time
        output_lines = []
        for line in iter(process.stdout.readline, ''):
            if not line:
                break

            line = line.strip()
            output_lines.append(line)

            # Log important lines
            if any(keyword in line for keyword in ['ERROR', 'WARNING', 'Crawling:', 'UPLOAD', 'PROGRESS', 'CLOSED']):
                logger.info(f"[SPIDER] {line}")

            # Extract progress info
            if 'PROGRESS' in line or 'Crawling:' in line:
                DatabaseOperations.insert_log(domain, line[:500], "INFO")

        # Wait for process to complete
        return_code = process.wait(timeout=3600)  # 1 hour timeout

        # Log final output
        logger.info(f"Spider process completed with return code: {return_code}")

        # Save last 50 lines of output
        for line in output_lines[-50:]:
            if line.strip():
                logger.info(f"[OUTPUT] {line}")

        # Check result
        if return_code == 0:
            final_progress = tracker.get_percentage()
            logger.info(f"✓ Crawl completed successfully for {domain}. Progress: {final_progress}%")
            DatabaseOperations.insert_log(domain, f"Crawl completed successfully", "INFO")
            DatabaseOperations.update_crawl_statistics(
                domain=domain,
                status='completed',
                end_time=datetime.now()
            )
        else:
            error_msg = f"Spider exited with code {return_code}"
            logger.error(f"✗ Crawl failed for {domain}: {error_msg}")

            # Log error details
            error_lines = [l for l in output_lines if 'ERROR' in l or 'Traceback' in l]
            for error_line in error_lines[-10:]:
                logger.error(f"[ERROR] {error_line}")

            DatabaseOperations.insert_log(domain, f"Crawl failed: {error_msg}", "ERROR")
            DatabaseOperations.update_crawl_statistics(
                domain=domain,
                status='failed',
                end_time=datetime.now()
            )

    except subprocess.TimeoutExpired:
        error_msg = "Spider execution timed out after 1 hour"
        logger.error(f"✗ {error_msg} for {domain}")
        DatabaseOperations.insert_log(domain, error_msg, "ERROR")
        DatabaseOperations.update_crawl_statistics(
            domain=domain,
            status='failed',
            end_time=datetime.now()
        )
        if process:
            process.kill()
            process.wait()

    except FileNotFoundError as e:
        error_msg = f"File not found: {str(e)}"
        logger.error(f"✗ {error_msg}")
        DatabaseOperations.insert_log(domain, error_msg, "ERROR")
        DatabaseOperations.update_crawl_statistics(
            domain=domain,
            status='failed',
            end_time=datetime.now()
        )

    except Exception as e:
        error_msg = str(e)
        logger.error(f"✗ Crawl task failed unexpectedly: {error_msg}", exc_info=True)
        DatabaseOperations.insert_log(domain, f"Task failed: {error_msg[:500]}", "ERROR")
        DatabaseOperations.update_crawl_statistics(
            domain=domain,
            status='failed',
            end_time=datetime.now()
        )

    finally:
        # Cleanup
        if domain in active_crawls:
            del active_crawls[domain]
        if process and process.poll() is None:
            try:
                logger.warning(f"Terminating hanging process for {domain}")
                process.terminate()
                process.wait(timeout=5)
            except:
                logger.error(f"Force killing process for {domain}")
                process.kill()


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            url = request.form.get('url')
            if not url:
                logger.error("No URL provided")
                return "No URL provided", 400

            logger.info(f"Received URL: {url}")

            # Verify URL
            verification = verify_url(url)
            domain = verification.get('domain')

            if not domain:
                logger.error("Failed to extract domain")
                return "Invalid URL - could not extract domain", 400

            logger.info(f"Domain extracted: {domain}")
            logger.info(f"Verification result: {verification}")

            # Check verification status
            if verification.get('dns_valid') and verification.get('socket_valid'):
                # Save SSL info
                DatabaseOperations.insert_ssl_info(
                    domain,
                    verification.get('ssl_provider', 'Unknown'),
                    verification.get('ssl_expiry')
                )

                # Initialize progress tracker
                ProgressTracker(domain).reset()

                # Log task initiation
                DatabaseOperations.insert_log(domain, f"Crawl task initiated for {url}", "INFO")

                # Start crawl in background thread
                thread = threading.Thread(target=run_crawl_task, args=(url, domain), daemon=True)
                thread.start()

                logger.info(f"Crawl thread started for domain: {domain}")

                return redirect(url_for('progress', domain=domain))

            else:
                error_msg = f"Verification failed - DNS: {verification.get('dns_valid')}, Socket: {verification.get('socket_valid')}"
                logger.error(error_msg)
                DatabaseOperations.insert_log(domain, error_msg, "ERROR")
                return error_msg, 400

        except Exception as e:
            logger.exception(f"Error processing request: {e}")
            return f"Internal error: {str(e)}", 500

    return render_template('index.html')


@app.route('/progress')
def progress():
    domain = request.args.get('domain')
    if not domain:
        return "Domain parameter missing", 400

    return render_template('progress.html', domain=domain)


@app.route('/get_progress')
def get_progress():
    try:
        domain = request.args.get('domain')
        if not domain:
            return jsonify({'error': 'Domain parameter missing'}), 400

        tracker = ProgressTracker(domain)
        progress = tracker.get_percentage()

        # Check crawl statistics for status
        stats = DatabaseOperations.get_crawl_statistics(domain)
        status = 'running'
        if stats:
            status = stats[4]  # status column

        return jsonify({
            'progress': progress,
            'status': status
        })

    except Exception as e:
        logger.error(f"Error getting progress: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/dashboard')
def dashboard():
    try:
        domain = request.args.get('domain')
        if not domain:
            return "Domain parameter missing", 400

        stats = DatabaseOperations.get_crawl_statistics(domain)
        logs = DatabaseOperations.get_recent_logs(domain, limit=100)

        return render_template('dashboard.html', domain=domain, stats=stats, logs=logs)

    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        return f"Error: {str(e)}", 500


if __name__ == '__main__':
    logger.info("Starting Flask application...")
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)
