if __name__ == '__main__':
    import time
    from threading import Lock
    from extensions.proxy import ProxySnatcher
    from finance import FinanceWorker
    from preprocess import PreProcessWorker
    from sentimentprocess import SentimentWorker
    from stream import StreamWorker
    from write import WriteWorker
    send_every = 60
    file_lock = Lock()
    proxy_thread = ProxySnatcher(110, **{'minDownloadSpeed': '50',
                                         'protocol': 'http',
                                         'allowsHttps': 1,
                                         'allowsUserAgentHeader': 1})
    proxy_thread.start()
    proxy_thread.wait_for_proxies()

    f = FinanceWorker(proxy_thread, send_every=send_every)
    f.start()
    time.sleep(1)

    s = StreamWorker(proxy_thread, use_processes=True)
    s.start()
    time.sleep(1)

    p = PreProcessWorker()
    p.start()
    time.sleep(1)

    st = SentimentWorker(send_every=send_every)
    st.start()
    time.sleep(1)

    w = WriteWorker(file_lock, send_every=send_every)
    w.start()
