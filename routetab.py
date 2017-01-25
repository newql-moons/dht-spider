from bitstring import BitArray
from config import k
from node import AbNode


class RouteTable(object):
    def __init__(self, node_id):
        self.node_id = BitArray(bytes=node_id)
        self.root = Bucket(self.node_id)
        self.__nodes = {}

    def insert(self, node):
        if not self.__nodes.get(node.addr):
            try:
                self.root = self.root.insert(node)
                self.__nodes[node.addr] = node
            except Trash:
                pass

    def get_neighbor(self, info_hash, n=k):
        info_hash = BitArray(bytes=info_hash)
        return self.root.get_neighbor(info_hash, n)[0]

    def __len__(self):
        return len(self.__nodes)

    def nodes(self):
        for node in self.root.nodes():
            yield node

    def buckets(self):
        if isinstance(self.root, Bucket):
            yield self.root
        else:
            for bucket in self.root.buckets():
                yield bucket

    def get_node(self, addr):
        try:
            return self.__nodes[addr]
        except KeyError:
            return AbNode(addr)


class TreeNode(object):
    def __init__(self, bucket):
        self.depth = bucket.depth
        self.node_id = bucket.node_id

        self.left = Bucket(self.node_id, self.depth + 1)
        self.right = Bucket(self.node_id, self.depth + 1)

        self.left.me_in = bucket.me_in and not self.node_id[self.depth]
        self.right.me_in = bucket.me_in and self.node_id[self.depth]

        for node in bucket:
            self.insert(node)

    def insert(self, node):
        if not node.id[self.depth]:
            self.left = self.left.insert(node)
        else:
            self.right = self.right.insert(node)
        return self

    def get_neighbor(self, info_hash, n=k):
        if not info_hash:
            a = self.left
            b = self.right
        else:
            a = self.right
            b = self.left
        nodes, num = a.get_neighbor(info_hash, n)
        if num < n:
            _nodes, _n = b.get_neighbor(info_hash, n - num)
            nodes.extend(_nodes)
            num += _n
        return nodes, num

    def nodes(self):
        for node in self.left.nodes():
            yield node
        for node in self.right.nodes():
            yield node

    def buckets(self):
        if isinstance(self.left, Bucket):
            yield self.left
        else:
            for bucket in self.left.buckets():
                yield bucket
        if isinstance(self.right, Bucket):
            yield self.right
        else:
            for bucket in self.right.buckets():
                yield bucket


class Bucket(object):
    def __init__(self, node_id, depth=0):
        self.node_id = node_id
        self.__nodes = []
        self.depth = depth
        self.me_in = True

    def insert(self, node):
        if len(self.__nodes) < k:
            self.__nodes.append(node)
            return self
        elif self.me_in:
            tn = TreeNode(self)
            tn.insert(node)
            return tn
        else:
            raise Trash()

    def get_neighbor(self, info_hash, n=k):
        size = len(self.__nodes)
        n = n if size >= n else size
        return self.__nodes[:n], n

    def __iter__(self):
        for node in self.__nodes:
            yield node

    def nodes(self):
        for node in self.__nodes:
            yield node


class Trash(Exception):
    pass
