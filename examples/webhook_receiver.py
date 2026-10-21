import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class WebhookReceiver(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        if self.path != "/webhook":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self.send_error(400)
            return
        if not 0 < length <= 65_536:
            self.send_error(413)
            return
        self.connection.settimeout(5)
        try:
            notification = json.loads(self.rfile.read(length))
        except (ValueError, TimeoutError):
            self.send_error(400)
            return
        print(json.dumps(notification, ensure_ascii=False), flush=True)
        self.send_response(204)
        self.end_headers()


if __name__ == "__main__":
    with HTTPServer(("0.0.0.0", 8080), WebhookReceiver) as server:
        server.serve_forever()
