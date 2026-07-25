"""Tests for the surviving custom-route API.

The standalone ``endpoints/`` subsystem (UDS worker, ``load_endpoints``,
``handle_request``, OpenAI specs) was removed; route handlers will be
re-integrated into the model worker in a later phase. What remains here:
``ServerProxy`` / ``RegistryProxy``, the ``@route`` decorator mechanic, and
package-level invariants. Route *discovery* coverage returns once the litapi
integration lands.
"""

import pytest

from lite_server.endpoint import router
from lite_server.server_proxy import RegistryProxy, ServerProxy


class TestRegistryProxy:
    def test_list_loaded(self):
        snapshot = {"loaded_models": [{"name": "m1", "version": "1"}]}
        reg = RegistryProxy(snapshot)
        assert reg.list_loaded() == [{"name": "m1", "version": "1"}]

    def test_list_loaded_empty(self):
        reg = RegistryProxy({})
        assert reg.list_loaded() == []


class TestServerProxy:
    def test_registry(self):
        snapshot = {"loaded_models": [{"name": "m1"}], "config": {"port": 8080}}
        srv = ServerProxy(snapshot)
        assert srv.registry.list_loaded() == [{"name": "m1"}]

    def test_config(self):
        snapshot = {"config": {"debug": True}}
        srv = ServerProxy(snapshot)
        assert srv.config["debug"] is True

    def test_config_default(self):
        srv = ServerProxy({})
        assert srv.config == {}


class TestRouterDecorator:
    """The ``@route`` (``router``) decorator survives the standalone-routes
    removal; this covers its registration mechanic until route discovery is
    rebuilt on top of the model worker."""

    def test_get_decorator_registers_route(self):
        router._routes.clear()
        try:
            @router.get("/x")
            def x(ctx):
                return {"ok": True}

            routes = router.routes
            assert len(routes) == 1
            assert routes[0].path == "/x"
            assert routes[0].methods == ["GET"]
            assert routes[0].handler is x
        finally:
            router._routes.clear()

    def test_post_decorator_registers_post_method(self):
        router._routes.clear()
        try:
            @router.post("/y")
            def y(ctx):
                return {"ok": True}

            assert router.routes[0].methods == ["POST"]
        finally:
            router._routes.clear()

    def test_routes_returns_independent_copy(self):
        router._routes.clear()
        try:
            @router.get("/z")
            def z(ctx):
                pass

            snapshot = router.routes
            snapshot.clear()
            assert len(router.routes) == 1  # mutating the copy must not touch the router
        finally:
            router._routes.clear()


class TestRequestModuleRemoved:
    """0.7.0: request.py is deleted — Request, URL, QueryParams, Client,
    State, UploadFile are no longer importable from lite_server."""

    def test_request_module_import_fails(self):
        with pytest.raises(ImportError):
            import lite_server.request  # noqa: F401

    def test_request_class_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import Request  # noqa: F401

    def test_url_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import URL  # noqa: F401

    def test_query_params_import_fails(self):
        with pytest.raises(ImportError):
            from lite_server import QueryParams  # noqa: F401

    def test_headers_still_importable_from_context(self):
        """Headers moves to lite_server.context, re-exported from lite_server."""
        from lite_server import Headers
        assert Headers is not None
