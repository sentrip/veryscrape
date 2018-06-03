from .scrapers import Twitter, Reddit, Google, Twingly, Spider
from .veryscrape import VeryScrape, register, unregister

__author__ = """Djordje Pepic"""
__email__ = 'djordje.m.pepic@gmail.com'
__version__ = '0.1.1'

register('twitter', Twitter)
register('reddit', Reddit)
register('article', Google)
register('blog', Twingly)
register('spider', Spider, classify=True)

__all__ = [
    'VeryScrape', 'register', 'unregister'
]
