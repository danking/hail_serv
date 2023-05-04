from typing import Any
import logging
import os

import hail as hl
import orjson
from aiohttp import web

from .logger import AccessLogger


N_CORES = 4
HTTP_CLIENT_MAX_SIZE = 8 * 1024 * 1024
LOG = logging.getLogger('hail_serv')
ROUTES = web.RouteTableDef()


async def on_startup(app: web.Application):
    del app
    hl.init(local=f'local[{N_CORES}]', backend='spark')


async def on_cleanup(app: web.Application):
    del app
    hl.stop()


async def json_request(request: web.Request) -> Any:
    return orjson.loads(await request.read())


def json_response(a: Any) -> web.Response:
    return web.json_response(body=orjson.dumps(a))


@ROUTES.post('/search')
async def search(request: web.Request) -> web.Response:
    request = await json_request(request)
    intervals = request.get('intervals', [])
    ht = hl.read_table('the-table.ht')
    if intervals:
        [hl.locus_interval(interval['chrom'], interval['start'], interval['end'])
         for interval in intervals]
        ht = hl.filter_intervals(ht, intervals)
    results = ht.collect()
    return json_response(results)


def run():
    app = web.Application(
        client_max_size=HTTP_CLIENT_MAX_SIZE
    )
    app.add_routes(ROUTES)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    web.run_app(
        app,
        host='0.0.0.0',
        port=int(os.environ['PORT']),
        access_log_class=AccessLogger,
    )
