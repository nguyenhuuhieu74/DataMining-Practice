"""Local read-only JSON API and web interface for the frozen recommender snapshots."""
import argparse
import csv
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import sys
from urllib.parse import parse_qs, urlparse
import zipfile

from pipeline_common import ROOT, ARTIFACTS
from recommend import SnapshotService, METHODS


def export_bundle(destination):
    """Package the exact serving inputs; never include hidden simulator profiles."""
    service = SnapshotService()
    destination = Path(destination).resolve()
    if destination.exists():
        raise FileExistsError('Bundle đã có, không ghi đè: ' + str(destination))
    project = ROOT.parent
    paths = set((ROOT / 'src').glob('*.py'))
    paths.update((ROOT / 'web').glob('*'))
    paths.update(ROOT / name for name in ['config.json', 'protocol.json', 'requirements.txt', 'README.md', 'DEMO.md'])
    for directory in ['models/primary', 'preprocessing', 'evaluation/primary']:
        paths.update(path for path in (ARTIFACTS / directory).rglob('*') if path.is_file())
    report_directory = ROOT / 'reports' / ARTIFACTS.name
    for name in ['REPORT.md', 'validation_primary.csv', 'test_primary_primary.csv',
                 'test_primary_primary_raw.csv', 'test_historical_primary.csv',
                 'selection_primary.json', 'preprocessing_summary.json']:
        report = report_directory / name
        if report.is_file():
            paths.add(report)
    for path in paths:
        if not path.is_file() or not path.resolve().is_relative_to(project.resolve()):
            raise ValueError('File bundle không hợp lệ: ' + str(path))
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, 'x', zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(paths):
            archive.write(path, path.relative_to(project).as_posix())
    with zipfile.ZipFile(destination) as archive:
        if archive.testzip() is not None:
            raise ValueError('Bundle không vượt kiểm tra ZIP')
    print('Đã đóng gói:', destination, '| file:', len(paths), '| MiB:', round(destination.stat().st_size / 2**20, 2))


def make_server(service, host='127.0.0.1', port=8501):
    page = (ROOT / 'web/index.html').read_bytes()

    class Handler(BaseHTTPRequestHandler):
        server_version = 'RecommendationDemo/1.0'

        def send_body(self, status, body, content_type='application/json; charset=utf-8', download=False):
            self.send_response(status)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Referrer-Policy', 'no-referrer')
            if download:
                self.send_header('Content-Disposition', 'attachment; filename="recommendations.csv"')
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status, payload):
            self.send_body(status, json.dumps(payload, ensure_ascii=False, allow_nan=False).encode('utf-8'))

        def do_GET(self):
            parsed = urlparse(self.path)
            if len(self.path) > 12000:
                self.send_json(414, {'error': 'URL quá dài'})
                return
            if parsed.path == '/':
                self.send_body(200, page, 'text/html; charset=utf-8')
                return
            if parsed.path == '/favicon.ico':
                self.send_body(204, b'', 'image/x-icon')
                return
            arguments = parse_qs(parsed.query, keep_blank_values=True)
            if any(len(values) != 1 for values in arguments.values()):
                self.send_json(400, {'error': 'Tham số không được lặp'})
                return
            def value(name, default=None):
                return arguments.get(name, [default])[0]
            try:
                source = value('source', 'pipeline')
                if parsed.path == '/api/status':
                    payload = service.status()
                elif parsed.path == '/api/health':
                    payload = {'status': 'ok', 'version': service.status()['version']}
                elif parsed.path == '/api/users':
                    payload = {'users': service.users(source, value('cohort'))}
                elif parsed.path == '/api/history':
                    payload = service.history(value('user_id'), source)
                elif parsed.path == '/api/metrics':
                    payload = service.evaluation(source, value('scope', 'primary'), value('mode', 'balanced'))
                elif parsed.path in ['/api/recommendations', '/api/compare', '/api/export']:
                    top_k = int(value('top_k', '10'))
                    options = dict(user_id=value('user_id'), method=value('method'), top_k=top_k,
                                   source=source, mode=value('mode', 'balanced'))
                    if parsed.path == '/api/compare':
                        methods = METHODS
                        payload = {'recommendations': [service.recommend(**{**options, 'method': method}) for method in methods]}
                    else:
                        payload = service.recommend(**options)
                    if parsed.path == '/api/export':
                        output = io.StringIO()
                        writer = csv.DictWriter(output, fieldnames=['rank', 'video_id', 'title', 'score', 'url'])
                        writer.writeheader()
                        for row in payload['items']:
                            # Keep text literal when opened in spreadsheet software.
                            exported = {key: row[key] for key in ['rank', 'video_id', 'title', 'score', 'url']}
                            for key in ['video_id', 'title']:
                                if exported[key].startswith(('=', '+', '-', '@', '\t', '\r')):
                                    exported[key] = "'" + exported[key]
                            writer.writerow(exported)
                        self.send_body(200, output.getvalue().encode('utf-8-sig'), 'text/csv; charset=utf-8', download=True)
                        return
                else:
                    self.send_json(404, {'error': 'Không có đường dẫn này'})
                    return
                self.send_json(200, payload)
            except (ValueError, TypeError, KeyError) as error:
                self.send_json(400, {'error': str(error)})
            except Exception:
                import traceback
                traceback.print_exc()
                self.send_json(500, {'error': 'Không đọc được snapshot. Xem terminal để kiểm tra dữ liệu.'})

        def do_POST(self):
            self.send_json(405, {'error': 'Demo chỉ đọc snapshot; chưa ghi phản hồi hoặc huấn luyện online'})

        def log_message(self, format_string, *arguments):
            print('HTTP', self.address_string(), format_string % arguments, flush=True)

    return ThreadingHTTPServer((host, port), Handler)


def main():
    parser = argparse.ArgumentParser(description='Web demo bốn phương pháp, dùng snapshot đã chốt')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8501)
    parser.add_argument('--export-bundle', type=Path)
    arguments = parser.parse_args()
    if arguments.export_bundle:
        export_bundle(arguments.export_bundle)
        return
    try:
        service = SnapshotService()
        server = make_server(service, arguments.host, arguments.port)
    except (OSError, ValueError, KeyError) as error:
        parser.exit(1, 'Không khởi động được demo: ' + str(error) + '\n')
    print(f'Demo: http://{arguments.host}:{arguments.port}', flush=True)
    print('Đã nạp snapshot; dùng Ctrl+C để dừng. Không cần Docker hoặc train lại.', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nĐã dừng demo.')
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
