import socket
import struct
import time

from bitstring import BitArray


class Node(object):
    def __init__(self, node_id, addr):
        self.id = BitArray(node_id)
        self.addr = addr
        self.dubious = False
        self.last_comm = time.time()

    @property
    def compact_info(self):
        node_id = self.id.bytes
        ip, port = self.addr
        ip = socket.inet_aton(ip)
        port = struct.pack('!H', port)
        return node_id + ip + port

    def __repr__(self):
        ip, port = self.addr
        return 'Node[%s %s:%d]' % (self.id.hex, ip, port)

    def comm(self):
        self.last_comm = time.time()

    def is_good(self):
        return time.time() - self.last_comm < 5 * 60


class AbNode(object):
    def __init__(self, addr):
        self.addr = addr

    @property
    def compact_info(self):
        return b''

    def __repr__(self):
        return '[%s:%d]' % self.addr


def pack_nodes(nodes):
    b_str = b''
    for node in nodes:
        b_str += node.compact_info
    return b_str


def unpack_nodes(b_str):
    nodes = []
    for i in range(len(b_str) // 26):
        info = b_str[i * 26: i * 26 + 26]
        node_id = info[:20]
        ip = socket.inet_ntoa(info[20:24])
        port = struct.unpack('!H', info[24:])[0]
        nodes.append(Node(node_id, (ip, port)))
    return nodes
