# -*- coding: utf-8 -*-

"""Console script for veryscrape."""

import asyncio
import logging
import click

from redis import Redis
from . import VeryScrape


@click.command('Run a local redis queue of social media data')
@click.option('--conf', default='scrape_config.json',
              help='Path to scrape config file.')
@click.option('--log-level', default='INFO',
              help='Log level for application.')
@click.option('--host', default='localhost',
              help='The interface to bind the redis server to.')
@click.option('--port', default=6379,
              help='The port to bind the redis server to.')
@click.option('--cores', default=1,
              help='The number of cores to use for processing text.')
def main(conf, log_level, host, port, cores):
    """Console script for veryscrape"""
    click.echo("")
    # todo: add startup logging

    queue = asyncio.Queue()
    scraper = VeryScrape(queue, config=conf, n_cores=cores)

    db = Redis(host=host, port=port)

    async def push_items():
        while not scraper.kill_event.is_set():
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                await asyncio.sleep(1e-3)
            else:
                db.rpush("events", "%s|%s|%s|%s" % (
                    item.source, item.topic, item.created_at, item.content
                ))
    # todo: add logging
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(scraper.scrape(), push_items()))
    loop.close()

    return 0
