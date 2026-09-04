import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import os

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"NSE OHLC Service Active")

    def log_message(self, format, *args):
        return  # Suppress server access logs to keep terminal clean

def run_http_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), SimpleHandler)
    server.serve_forever()

def start_background_http():
    thread = threading.Thread(target=run_http_server, daemon=True)
    thread.start()