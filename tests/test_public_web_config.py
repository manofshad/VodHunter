from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


def test_public_web_client_defaults_to_same_origin_api() -> None:
    client_source = (ROOT_DIR / "web-public/src/api/client.ts").read_text()

    assert 'const PROD_API_BASE = "/api";' in client_source
    assert 'const API_BASE = ENV_API_BASE || (import.meta.env.DEV ? DEV_API_BASE : PROD_API_BASE);' in client_source
    assert "VITE_API_BASE is required in production" not in client_source


def test_nginx_public_template_proxies_api_and_rate_limits_search() -> None:
    nginx_template = (ROOT_DIR / "nginx.public.conf.template").read_text()

    assert "limit_req_zone $binary_remote_addr zone=public_search:10m rate=6r/m;" in nginx_template
    assert "location = /api/search/clip {" in nginx_template
    assert "limit_req zone=public_search burst=2;" in nginx_template
    assert "location /api/ {" in nginx_template
    assert "proxy_pass ${PUBLIC_API_UPSTREAM};" in nginx_template
    assert 'return 429 \'{"detail":{"code":"RATE_LIMITED","message":"Too many search requests. Please wait a moment and try again."}}\';' in nginx_template


def test_public_web_dockerfile_uses_templated_nginx_config() -> None:
    dockerfile = (ROOT_DIR / "Dockerfile.web-public").read_text()

    assert "ENV PUBLIC_API_UPSTREAM=http://api-public:8000" in dockerfile
    assert "COPY nginx.public.conf.template /etc/nginx/templates/default.conf.template" in dockerfile
