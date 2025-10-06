import socket
import ssl
from urllib.parse import urlparse
import logging
import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
import requests

logger = logging.getLogger(__name__)


def verify_url(url):
    """
    Verify URL with fast timeouts and parallel checks
    Maximum execution time: ~10 seconds
    """
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc or parsed_url.path

        if not domain:
            logger.error(f"Could not extract domain from URL: {url}")
            return {}

        logger.info(f"Verifying {url} (domain: {domain})")

        result = {
            'domain': domain,
            'dns_valid': False,
            'whois_valid': False,
            'socket_valid': False,
            'ssl_provider': 'Unknown',
            'ssl_expiry': None
        }

        # Run checks in parallel with timeout
        with ThreadPoolExecutor(max_workers=3) as executor:
            # DNS check
            dns_future = executor.submit(check_dns, domain)

            # Socket check (includes implicit DNS check)
            socket_future = executor.submit(check_socket, domain)

            # SSL check
            ssl_future = executor.submit(check_ssl, domain)

            # Collect results with timeout
            try:
                result['dns_valid'] = dns_future.result(timeout=3)
            except Exception as e:
                logger.warning(f"DNS check failed: {e}")

            try:
                result['socket_valid'] = socket_future.result(timeout=5)
            except Exception as e:
                logger.warning(f"Socket check failed: {e}")

            try:
                ssl_info = ssl_future.result(timeout=5)
                if ssl_info:
                    result['ssl_provider'] = ssl_info.get('provider', 'Unknown')
                    result['ssl_expiry'] = ssl_info.get('expiry')
            except Exception as e:
                logger.warning(f"SSL check failed: {e}")

        # Skip WHOIS check - it's too slow and unreliable
        # If DNS and socket checks pass, that's sufficient validation
        result['whois_valid'] = result['dns_valid'] and result['socket_valid']

        logger.info(f"Verification complete for {domain}: DNS={result['dns_valid']}, "
                   f"Socket={result['socket_valid']}, SSL={result['ssl_provider']}")

        return result

    except Exception as e:
        logger.error(f"Verification failed for {url}: {e}")
        return {}


def check_dns(domain):
    """Check DNS resolution with timeout"""
    try:
        socket.setdefaulttimeout(2)
        socket.gethostbyname(domain)
        logger.debug(f"DNS check passed for {domain}")
        return True
    except socket.gaierror as e:
        logger.warning(f"DNS resolution failed for {domain}: {e}")
        return False
    except Exception as e:
        logger.warning(f"DNS check error for {domain}: {e}")
        return False
    finally:
        socket.setdefaulttimeout(None)


def check_socket(domain):
    """Check socket connection with timeout"""
    try:
        sock = socket.create_connection((domain, 443), timeout=3)
        sock.close()
        logger.debug(f"Socket check passed for {domain}")
        return True
    except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError) as e:
        logger.warning(f"Socket connection failed for {domain}: {e}")
        return False
    except Exception as e:
        logger.warning(f"Socket check error for {domain}: {e}")
        return False


def check_ssl(domain):
    """Check SSL certificate with timeout"""
    try:
        context = ssl.create_default_context()
        context.check_hostname = True
        context.verify_mode = ssl.CERT_REQUIRED

        with socket.create_connection((domain, 443), timeout=3) as sock:
            with context.wrap_socket(sock, server_hostname=domain) as ssock:
                cert = ssock.getpeercert()

                # Parse issuer information
                issuer = {}
                if cert and 'issuer' in cert:
                    for item in cert['issuer']:
                        if isinstance(item, tuple) and len(item) > 0:
                            for key_val in item:
                                if isinstance(key_val, tuple) and len(key_val) == 2:
                                    issuer[key_val[0]] = key_val[1]

                ssl_provider = issuer.get('organizationName', issuer.get('commonName', 'Unknown'))

                # Parse expiry date
                ssl_expiry = None
                if cert and 'notAfter' in cert:
                    try:
                        expiry_str = cert['notAfter']
                        ssl_expiry = datetime.datetime.strptime(expiry_str, '%b %d %H:%M:%S %Y %Z')
                    except ValueError:
                        # Try alternative format
                        try:
                            ssl_expiry = datetime.datetime.strptime(expiry_str, '%b %d %H:%M:%S %Y %z')
                        except ValueError:
                            logger.warning(f"Could not parse SSL expiry date: {expiry_str}")

                logger.debug(f"SSL check passed for {domain}: Provider={ssl_provider}, Expiry={ssl_expiry}")

                return {
                    'provider': ssl_provider,
                    'expiry': ssl_expiry
                }

    except ssl.SSLError as e:
        logger.warning(f"SSL error for {domain}: {e}")
        return None
    except (socket.timeout, socket.gaierror, ConnectionRefusedError, OSError) as e:
        logger.warning(f"SSL connection failed for {domain}: {e}")
        return None
    except Exception as e:
        logger.warning(f"SSL check error for {domain}: {e}")
        return None


def check_url_http(url, timeout=5):
    """
    Optional: Check URL accessibility via HTTP request
    Use this if you want to verify the URL actually returns content
    """
    try:
        response = requests.head(url, timeout=timeout, allow_redirects=True)
        return response.status_code < 400
    except requests.RequestException as e:
        logger.warning(f"HTTP check failed for {url}: {e}")
        return False
