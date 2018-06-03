# -*- coding: utf-8 -*-

"""Console script for veryscrape."""

import asyncio
import logging
import logging.handlers
import sys
import click

from redis import Redis
from . import VeryScrape


@click.command('Run a local redis queue of social media data')
@click.option('--conf', default='scrape_config.json',
              help='Path to scrape config file.')
@click.option('--host', default='localhost',
              help='The interface to bind the redis server to.')
@click.option('--port', default=6379,
              help='The port to bind the redis server to.')
@click.option('--cores', default=1,
              help='The number of cores to use for processing text.'
                   'Pass --cores -1 to disable processing of text.'
                   'Pass --cores 0 to use all available cores.')
@click.option('--log-level', default='INFO',
              help='Log level for application.')
@click.option('--log-file', default=None,
              help='File to output application logs '
                   '(logs go to stdout if this is None)')
@click.option('--max-log-size', default=1024 * 1024,
              help='Max size in bytes for the log file, if one is specified.')
def main(conf, host, port, cores, log_level, log_file, max_log_size):
    """Console script for veryscrape"""
    click.echo("Setting up VeryScrape redis queue...")

    queue = asyncio.Queue()
    scraper = VeryScrape(queue)
    db = Redis(host=host, port=port)

    # Setup logging
    logger = logging.getLogger('veryscrape')
    logger.setLevel(log_level)
    if log_file is None:
        handler = logging.StreamHandler(stream=sys.stdout)
    else:
        handler = logging.handlers.RotatingFileHandler(log_file, max_log_size)
    handler.setLevel(log_level)
    # todo make log format
    # handler.setFormatter(logging.Formatter(
    #     fmt='', datefmt=''
    # ))
    logger.addHandler(handler)

    # Scrape and push items to redis
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(
        scraper.scrape(conf, n_cores=cores),
        _push_items(scraper, queue, db)
    ))
    loop.close()

    return 0


async def _push_items(scraper, queue, db):
    while not scraper.kill_event.is_set():
        try:
            item = queue.get_nowait()
        except asyncio.QueueEmpty:
            await asyncio.sleep(1e-3)
        else:
            db.rpush("events", "%s|%s|%s|%s" % (
                item.source, item.topic, item.created_at, item.content
            ))
