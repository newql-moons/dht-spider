import threading
import queue
import logging
import sys

from util import randomid, bencode, threadpool
from routetab import RouteTable
from config import addr, max_size, start_url
from node import *


class Spider(object):
    def __init__(self):
        self.node_id = randomid()
        self.route_table = RouteTable(self.node_id)
        self.hash_buf = HashBuff(20)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(addr)

        self.send_worker = SendWorker(self)
        self.recv_worker = RecvWorker(self)

    def start(self):
        self.send_worker.start()
        self.recv_worker.start()
        self.run()

    def run(self):
        self.join_dht()
        clock = time.time()
        while True:
            try:
                args = self.recv_worker.recv()
                if args:
                    self.msg_handler(*args)
            except Exception:
                logging.error(sys.exc_info()[:2])
            if time.time() - clock > 5 * 60:
                self.route_table.fresh(self.ping, self.find_node)

    def join_dht(self):
        for url in start_url:
            node = AbNode(url)
            self.find_node(node, self.node_id)

    def msg_handler(self, msg, node):
        if isinstance(node, Node):
            self.route_table.update(node)
        if msg.get(b'e'):
            return
        try:
            t = msg[b't']
        except KeyError as e:
            logging.error(msg)
            raise e
        y = msg[b'y']
        if y == b'q':
            self.req_handle(node, t, msg[b'q'], msg[b'a'])
        elif y == b'r':
            self.resp_handler(node, msg[b'r'])
        else:
            pass

    def req_handle(self, node, transaction_id, q, a):
        if isinstance(node, AbNode):
            self.route_table.insert(Node(a[b'id'], node.addr))
            node = self.route_table.get_node(node.addr)
        logging.debug('Recv req(%s) from %s' % (q.decode(), node))

        def ping():
            r = {b'id': self.node_id}
            logging.debug('Send resp(ping) to %s' % node)
            self.resp(node, transaction_id, r)

        def find_node():
            target = a[b'target']
            nodes = self.route_table.get_neighbor(target)
            r = {
                b'id': self.node_id,
                b'nodes': pack_nodes(nodes),
            }
            logging.debug('Send resp(find_node) to %s' % node)
            self.resp(node, transaction_id, r)

        def get_peers():
            info_hash = a[b'info_hash']
            self.hash_buf.put(info_hash)
            nodes = self.route_table.get_neighbor(info_hash)
            r = {
                b'id': self.node_id,
                b'nodes': pack_nodes(nodes),
                b'token': randomid(8),
            }
            logging.debug('Send resp(get_peers) to %s' % node)
            self.resp(node, transaction_id, r)
            for nd in self.route_table.nodes():
                self.get_peers(nd, info_hash)

        def announce_peer():
            r = {b'id': self.node_id}
            self.resp(node, transaction_id, r)
            # with open('magnet.txt', 'a') as fp:
            #     fp.write(a[b'info_hash'])
            logging.info(a[b'info_hash'].hex())

        handlers = {
            b'ping': ping,
            b'find_node': find_node,
            b'get_peers': get_peers,
            b'announce_peer': announce_peer,
        }
        try:
            handlers[q]()
        except KeyError as e:
            raise e

    def resp_handler(self, node, r):
        if isinstance(node, AbNode):
            self.route_table.insert(Node(r[b'id'], node.addr))
            node = self.route_table.get_node(node.addr)
        try:
            nodes = unpack_nodes(r[b'nodes'])
            for nd in nodes:
                self.route_table.insert(nd)
                self.ask(nd)
            logging.info('Get %d nodes from %s, you already have %d nodes'
                         % (len(nodes), node, len(self.route_table)))
        except KeyError:
            pass

    def ask(self, node):
        if self.route_table.__len__() < max_size:
            self.find_node(node, self.node_id)
        # for info_hash in self.hash_buf:
        #     self.get_peers(node, info_hash)

    def req(self, node, q, a):
        msg = {
            b'y': b'q',
            b'q': q,
            b'a': a,
            b't': randomid(2)
        }
        logging.debug('Send req(%s) to %s' % (q.decode(), node))
        self.send_worker.send(msg, node)

    def resp(self, node, t, r):
        msg = {
            b'y': b'r',
            b't': t,
            b'r': r,
        }
        self.send_worker.send(msg, node)

    def ping(self, node):
        q = b'ping'
        a = {b'id': self.node_id}
        self.req(node, q, a)

    def find_node(self, node, target):
        q = b'find_node'
        a = {
            b'id': self.node_id,
            b'target': target
        }
        self.req(node, q, a)

    def get_peers(self, node, info_hash):
        q = b'get_peers'
        a = {
            b'id': self.node_id,
            b'info_hash': info_hash
        }
        self.req(node, q, a)


class SendWorker(threading.Thread):
    def __init__(self, spider):
        super().__init__()
        self.spider = spider
        self.buf = queue.Queue()
        self.pool = threadpool.ThreadPool(10)

    def run(self):
        while True:
            data, addr = self.buf.get()
            # self.spider.sock.sendto(data, addr)
            self.pool.add_task(self.spider.sock.sendto, data, addr)
            self.buf.task_done()

    def send(self, msg, node):
        self.buf.join()
        data = bencode.dumps(msg)
        self.buf.put((data, node.addr))


class RecvWorker(threading.Thread):
    def __init__(self, spider):
        super().__init__()
        self.spider = spider
        self.buf = queue.Queue()
        pass

    def run(self):
        while True:
            self.buf.join()
            data, addr = self.spider.sock.recvfrom(65535)
            # print(data)
            self.buf.put((data, addr))

    def recv(self):
        try:
            data, addr = self.buf.get_nowait()
            msg = bencode.loads(data)
            node = self.spider.route_table.get_node(addr)
            self.buf.task_done()
            return msg, node
        except queue.Empty:
            return None


class HashBuff(object):
    def __init__(self, size=5):
        self.sum = 0
        self.buf = []
        self.__size = size

    def put(self, info_hash):
        if info_hash not in self.buf:
            self.sum += 1
            if len(self.buf) < self.__size:
                self.buf.append(info_hash)
            else:
                self.buf.pop()
                self.buf.append(info_hash)

    def __iter__(self):
        for info_hash in self.buf:
            yield info_hash
