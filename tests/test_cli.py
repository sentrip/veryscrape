from threading import Timer
from _thread import interrupt_main
from veryscrape.cli import main
import pytest


def test_main(patched_aiohttp, temp_config, patched_redis):
    Timer(1e-1, interrupt_main).start()
    with pytest.raises(SystemExit):
        main([])

    assert len(patched_redis.data['events']) == 4, 'Did not get all items'
    while len(patched_redis.data['events']):
        _, topic, _, content = patched_redis.lpop('events').split('|')
        assert topic == 'topic1', 'Incorrect topic for item'
        assert 'some data' in content, 'Data did not pass through cleaning'
