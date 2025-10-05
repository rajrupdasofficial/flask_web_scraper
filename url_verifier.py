import socket
import ssl
import whois
from urllib.parse import urlparse
import logging
import datetime

logger = logging.getLogger(__name__)

def verify_url(url):
    try:
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        logger.info(f"Verifying {url} ({domain})")

        # DNS
        dns_valid = True
        try:
            socket.gethostbyname(domain)
        except:
            dns_valid = False

        # Whois
        whois_valid = True
        try:
            w = whois.whois(domain)
            if not w.domain_name:
                whois_valid = False
        except:
            whois_valid = False

        # Socket
        socket_valid = True
        try:
            sock = socket.create_connection((domain, 443), timeout=5)
            sock.close()
        except:
            socket_valid = False

        # SSL
        ssl_provider = 'Unknown'
        ssl_expiry = None
        try:
            context = ssl.create_default_context()
            with socket.create_connection((domain, 443)) as sock:
                with context.wrap_socket(sock, server_hostname=domain) as ssock:
                    cert = ssock.getpeercert()
                    # Fixed parsing: Handle different cert formats
                    issuer = dict(x[0] for x in cert.get('issuer', ())) if cert.get('issuer') else {}
                    ssl_provider = issuer.get('organizationName', 'Unknown')
                    expiry_str = cert.get('notAfter')
                    if expiry_str:
                        ssl_expiry = datetime.datetime.strptime(expiry_str, '%b %d %H:%M:%S %Y %Z')
        except Exception as e:
            logger.warning(f"SSL failed: {e}")

        return {
            'domain': domain,
            'dns_valid': dns_valid,
            'whois_valid': whois_valid,
            'socket_valid': socket_valid,
            'ssl_provider': ssl_provider,
            'ssl_expiry': ssl_expiry
        }
    except Exception as e:
        logger.error(f"Verify failed: {e}")
        return {}
