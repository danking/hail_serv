import asyncio
import logging
import aiohttp
from aiohttp import web


HTTP_CLIENT_MAX_SIZE = 8 * 1024 * 1024
LOG = logging.getLogger('hail_serv')
ROUTES = web.RouteTableDef()


def run():
    app = web.Application(
        client_max_size=HTTP_CLIENT_MAX_SIZE
    )
    app.add_routes(routes)
    app.router.add_get("/metrics", server_stats)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    asyncio.get_event_loop().add_signal_handler(signal.SIGUSR1, dump_all_stacktraces)

    web.run_app(
        deploy_config.prefix_application(app, 'batch', client_max_size=HTTP_CLIENT_MAX_SIZE),
        host='0.0.0.0',
        port=int(os.environ['PORT']),
        access_log_class=BatchFrontEndAccessLogger,
        ssl_context=internal_server_ssl_context(),
    )
