from typing import Any
import logging
import asyncio
import os

import hail as hl
import orjson
from aiohttp import web

from .logger import AccessLogger


N_CORES = 4
HTTP_CLIENT_MAX_SIZE = 8 * 1024 * 1024
LOG = logging.getLogger('hail_serv')


async def on_cleanup(app: web.Application):
    del app
    hl.stop()


async def json_request(request: web.Request) -> Any:
    return orjson.loads(await request.read())


def json_dump_special_classes(a: Any) -> Any:
    if isinstance(a, hl.Call):
        return str(a)
    raise ValueError(f'unknown type: {type(a)} {a}')


def json_response(a: Any) -> web.Response:
    return web.json_response(body=orjson.dumps(a, default=json_dump_special_classes))


class HailServ:
    def __init__(self):
        hl.init(local=f'local[{N_CORES}]', backend='spark')
        self.mt = hl.read_matrix_table('the-dataset.mt')
        self.hail_lock = asyncio.Lock()

    async def search(self, request: web.Request) -> web.Response:
        async with self.hail_lock:
            mt = self.mt
            request = await json_request(request)
            intervals = request.get('intervals', [])
            if intervals:
                mt = hl.filter_intervals(
                    mt,
                    [
                        hl.locus_interval(interval['chrom'], interval['start'], interval['end'])
                        for interval in intervals
                    ]
                )
            results = mt.GT.collect()
            return json_response(results)


def run():
    hail_serv = HailServ()
    app = web.Application(
        client_max_size=HTTP_CLIENT_MAX_SIZE
    )
    app.on_cleanup.append(on_cleanup)
    app.add_routes([web.post('/search', hail_serv.search)])
    web.run_app(
        app,
        host='0.0.0.0',
        port=int(os.environ['PORT']),
        access_log_class=AccessLogger,
    )
