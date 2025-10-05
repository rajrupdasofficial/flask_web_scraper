from flask import Flask, request, render_template, redirect, url_for, jsonify
import logging
from url_verifier import verify_url
from operations import DatabaseOperations
from celery_app import crawl_task
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

                # Start Celery task
                task = crawl_task.delay({'url': url, 'domain': domain})
                logger.info(f"Celery task started: {task.id}")

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

        return jsonify({
            'progress': progress,
            'status': 'running' if progress < 100 else 'completed'
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
        logs = DatabaseOperations.get_recent_logs(domain)

        return render_template('dashboard.html', domain=domain, stats=stats, logs=logs)
    except Exception as e:
        logger.error(f"Error loading dashboard: {e}")
        return f"Error: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
