import sys
import queue
import threading


class _Worker(threading.Thread):
    def __init__(self, wait_queue, result_queue, err_queue):
        super().__init__()

        self.wait_queue = wait_queue
        self.result_queue = result_queue
        self.err_queue = err_queue

        self.setDaemon(True)
        self.start()

    def run(self):
        while True:
            cmd, callback, args, kwargs = self.wait_queue.get()
            if cmd == 'stop':
                break
            elif cmd == 'process':
                try:
                    self.result_queue.put(callback(*args, **kwargs))
                except:
                    self.report_error()

    def report_error(self):
        self.err_queue.put(sys.exc_info()[:2])

    def dismiss(self):
        cmd = 'stop'
        self.wait_queue.put((cmd, None, None, None,))


class ThreadPool(object):
    def __init__(self, max_num):
        self.wait_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.err_queue = queue.Queue()

        self.workers = []
        for i in range(max_num):
            worker = _Worker(self.wait_queue, self.result_queue, self.err_queue)
            self.workers.append(worker)

    def add_task(self, callback, *args, **kwargs):
        cmd = 'process'
        self.wait_queue.put((cmd, callback, args, kwargs,))

    def get_result(self):
        return self.result_queue.get()

    def results(self, block=True):
        try:
            while True:
                yield self.result_queue.get(block)
        except queue.Empty:
            raise StopIteration

    def destroy(self):
        for worker in self.workers:
            worker.dismiss()
        for worker in self.workers:
            worker.join()

    def show_errors(self):
        try:
            while True:
                print(self.err_queue.get_nowait())
        except queue.Empty:
            pass
