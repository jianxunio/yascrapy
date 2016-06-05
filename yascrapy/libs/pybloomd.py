"""
This module implements a client for the BloomD server.
"""
__all__ = ["BloomdError", "BloomdConnection", "BloomdClient", "BloomdFilter"]
__version__ = "0.4.6"
import logging
import socket
import errno
import time
import hashlib

# Check for TCP_NODELAY support
HAS_TCP_NODELAY = hasattr(socket, "TCP_NODELAY")


class BloomdError(Exception):
    "Root of exceptions from the client library"
    pass


class BloomdConnection(object):
    "Provides a convenient interface to server connections"
    def __init__(self, server, timeout, attempts=3):
        """
        Creates a new Bloomd Connection.

        :Parameters:
            - server: Provided as a string, either as "host" or "host:port" or "host:port:udpport".
                      Uses the default port of 8673 if none is provided for tcp, and 8674 for udp.
            - timeout: The socket timeout to use.
            - attempts (optional): Maximum retry attempts on errors. Defaults to 3.
        """
        # Parse the host/port
        parts = server.split(":", 1)
        if len(parts) == 2:
            host, port = parts[0], int(parts[1])
        else:
            host, port = parts[0], 8673

        self.server = (host, port)
        self.timeout = timeout
        self.sock = None
        self.fh = None
        self.attempts = attempts
        self.logger = logging.getLogger("pybloomd.BloomdConnection.%s.%d" % self.server)

    def _create_socket(self):
        "Creates a new socket, tries to connect to the server"
        # Connect the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self.server)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        # Set no delay if possible
        if HAS_TCP_NODELAY:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.fh = None
        return s

    def send(self, cmd):
        "Sends a command with out the newline to the server"
        if self.sock is None:
            self.sock = self._create_socket()
        sent = False
        for attempt in xrange(self.attempts):
            try:
                self.sock.sendall(cmd + "\n")
                sent = True
                break
            except socket.error as e:
                self.logger.exception("Failed to send command to bloomd server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not sent:
            self.logger.critical("Failed to send command to bloomd server after %d attempts!" % self.attempts)
            raise EnvironmentError("Cannot contact bloomd server!")

    def read(self):
        "Returns a single line from the file"
        if self.sock is None:
            self.sock = self._create_socket()
        if not self.fh:
            self.fh = self.sock.makefile()
        read = self.fh.readline().rstrip("\r\n")
        return read

    def readblock(self, start="START", end="END"):
        """
        Reads a response block from the server. The servers
        responses are between `start` and `end` which can be
        optionally provided. Returns an array of the lines within
        the block.
        """
        lines = []
        first = self.read()
        if first != start:
            raise BloomdError("Did not get block start (%s)! Got '%s'!" % (start, first))
        while True:
            line = self.read()
            if line == "":
                raise BloomdError("Did not get block end! Got blank response.")
            if line == end:
                break
            lines.append(line)
        return lines

    def send_and_receive(self, cmd):
        """
        Convenience wrapper around `send` and `read`. Sends a command,
        and reads the response, performing a retry if necessary.
        """
        done = False
        for attempt in xrange(self.attempts):
            try:
                self.send(cmd)
                return self.read()
            except socket.error as e:
                self.logger.exception("Failed to send command to bloomd server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not done:
            self.logger.critical("Failed to send command to bloomd server after %d attempts!" % self.attempts)
            raise EnvironmentError("Cannot contact bloomd server!")

    def response_block_to_dict(self):
        """
        Convenience wrapper around `readblock` to convert a block
        output into a dictionary by splitting on spaces, and using the
        first column as the key, and the remainder as the value.
        """
        resp_lines = self.readblock()
        return dict(tuple(l.split(" ", 1)) for l in resp_lines)


class BloomdClient(object):
    "Provides a client abstraction around the BloomD interface."
    def __init__(self, servers, timeout=None, hash_keys=False):
        """
        Creates a new BloomD client.

        :Parameters:
            - servers : A list of servers, which are provided as strings in the "host" or "host:port".
            - timeout: (Optional) A socket timeout to use, defaults to no timeout.
            - hash_keys: (Optional) Should keys be hashed before sending to bloomd. Defaults to False.
        """
        if len(servers) == 0:
            raise ValueError("Must provide at least 1 server!")
        self.servers = servers
        self.timeout = timeout
        self.server_conns = {}
        self.server_info = None
        self.info_time = 0
        self.hash_keys = hash_keys

    def _server_connection(self, server):
        "Returns a connection to a server, tries to cache connections."
        if server in self.server_conns:
            return self.server_conns[server]
        else:
            conn = BloomdConnection(server, self.timeout)
            self.server_conns[server] = conn
            return conn

    def _get_connection(self, filter, strict=True, explicit_server=None):
        """
        Gets a connection to a server which is able to service a filter.
        Because filters may exist on any server, all servers are queried
        for their filters and the results are cached. This allows us to
        partition data across multiple servers.

        :Parameters:
            - filter : The filter to connect to
            - strict (optional) : If True, an error is raised when a filter
                does not exist.
            - explicit_server (optional) : If provided, when a filter does
                not exist and strict is False, a connection to this server is made.
                Otherwise, the server with the fewest sets is returned.
        """
        # Force checking if we have no info or 5 minutes has elapsed
        if not self.server_info or time.time() - self.info_time > 300:
            self.server_info = self.list_filters(inc_server=True)
            self.info_time = time.time()

        # Check if this filter is in a known location
        if filter in self.server_info:
            serv = self.server_info[filter][0]
            return self._server_connection(serv)

        # Possibly old data? Reload
        self.server_info = self.list_filters(inc_server=True)
        self.info_time = time.time()

        # Recheck
        if filter in self.server_info:
            serv = self.server_info[filter][0]
            return self._server_connection(serv)

        # Check if this is fatal
        if strict:
            raise BloomdError("Filter does not exist!")

        # We have an explicit server provided to us, use that
        if explicit_server:
            return self._server_connection(explicit_server)

        # Does not exist, and is not not strict
        # we can select a server on any criteria then.
        # We will use the server with the minimal set count.
        counts = {}
        for server in self.servers:
            counts[server] = 0
        for filter, (server, info) in self.server_info.items():
            counts[server] += 1

        counts = [(count, srv) for srv, count in counts.items()]
        counts.sort()

        # Select the least used
        serv = counts[0][1]
        return self._server_connection(serv)

    def create_filter(self, name, capacity=None, prob=None, in_memory=False, server=None):
        """
        Creates a new filter on the BloomD server and returns a BloomdFilter
        to interface with it. This will return a BloomdFilter object attached
        to the filter if the filter already exists.

        :Parameters:
            - name : The name of the new filter
            - capacity (optional) : The initial capacity of the filter
            - prob (optional) : The inital probability of false positives.
            - in_memory (optional) : If True, specified that the filter should be created
              in memory only.
            - server (optional) : In a multi-server environment, this forces the
                    filter to be created on a specific server. Should be provided
                    in the same format as initialization "host" or "host:port".
        """
        if prob and not capacity:
            raise ValueError("Must provide size with probability!")
        conn = self._get_connection(name, strict=False, explicit_server=server)
        cmd = "create %s" % name
        if capacity:
            cmd += " capacity=%d" % capacity
        if prob:
            cmd += " prob=%f" % prob
        if in_memory:
            cmd += " in_memory=1"
        conn.send(cmd)
        resp = conn.read()
        if resp == "Done":
            return BloomdFilter(conn, name, self.hash_keys)
        elif resp == "Exists":
            return self[name]
        else:
            raise BloomdError("Got response: %s" % resp)

    def __getitem__(self, name):
        "Gets a BloomdFilter object based on the name."
        conn = self._get_connection(name)
        return BloomdFilter(conn, name, self.hash_keys)

    def list_filters(self, prefix=None, inc_server=False):
        """
        Lists all the available filters across all servers.
        Returns a dictionary of {filter_name : filter_info}.

        :Parameters:
            - prefix (optional) : If provided, only list matching the prefix
            - inc_server (optional) : If true, the dictionary values
               will be (server, filter_info) instead of filter_info.
        """
        if prefix:
            cmd = "list %s" % prefix
        else:
            cmd = "list"

        # Send the list to all first
        for server in self.servers:
            conn = self._server_connection(server)
            conn.send(cmd)

        # Check response from all
        responses = {}
        for server in self.servers:
            conn = self._server_connection(server)
            resp = conn.readblock()
            for line in resp:
                name, info = line.split(" ", 1)
                if inc_server:
                    responses[name] = server, info
                else:
                    responses[name] = info

        return responses

    def flush(self):
        "Instructs all servers to flush to disk"
        # Send the flush to all first
        for server in self.servers:
            conn = self._server_connection(server)
            conn.send("flush")

        # Check response from all
        for server in self.servers:
            conn = self._server_connection(server)
            resp = conn.read()
            if resp != "Done":
                raise BloomdError("Got response: '%s' from '%s'" % (resp, server))


class BloomdFilter(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, conn, name, hash_keys=False):
        """
        Creates a new BloomdFilter object.

        :Parameters:
            - conn : The connection to use
            - name : The name of the filter
            - hash_keys : Should the keys be hashed client side
        """
        self.conn = conn
        self.name = name
        self.hash_keys = hash_keys

    def _get_key(self, key):
        """
        Returns the key we should send to the server
        """
        if self.hash_keys:
            return hashlib.sha1(key).hexdigest()
        return key

    def add(self, key):
        """
        Adds a new key to the filter. Returns True/False if the key was added.
        """
        resp = self.conn.send_and_receive("s %s %s" % (self.name, self._get_key(key)))
        if resp in ("Yes", "No"):
            return resp == "Yes"
        raise BloomdError("Got response: %s" % resp)

    def bulk(self, keys):
        "Performs a bulk set command, adds multiple keys in the filter"
        command = ("b %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        resp = self.conn.send_and_receive(command)

        if resp[:3] == "Yes" or resp[:2] == "No":
            return [r == "Yes" for r in resp.split(" ")]
        raise BloomdError("Got response: %s" % resp)

    def drop(self):
        "Deletes the filter from the server. This is permanent"
        resp = self.conn.send_and_receive("drop %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def close(self):
        """
        Closes the filter on the server.
        """
        resp = self.conn.send_and_receive("close %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def clear(self):
        """
        Clears the filter on the server.
        """
        resp = self.conn.send_and_receive("clear %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def __contains__(self, key):
        "Checks if the key is contained in the filter."
        resp = self.conn.send_and_receive("c %s %s" % (self.name, self._get_key(key)))
        if resp in ("Yes", "No"):
            return resp == "Yes"
        raise BloomdError("Got response: %s" % resp)

    def multi(self, keys):
        "Performs a multi command, checks for multiple keys in the filter"
        command = ("m %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        resp = self.conn.send_and_receive(command)

        if resp[:3] == "Yes" or resp[:2] == "No":
            return [r == "Yes" for r in resp.split(" ")]
        raise BloomdError("Got response: %s" % resp)

    def __len__(self):
        "Returns the count of items in the filter."
        info = self.info()
        return int(info["size"])

    def info(self):
        "Returns the info dictionary about the filter."
        self.conn.send("info %s" % (self.name))
        return self.conn.response_block_to_dict()

    def flush(self):
        "Forces the filter to flush to disk"
        resp = self.conn.send_and_receive("flush %s" % (self.name))
        if resp != "Done":
            raise BloomdError("Got response: %s" % resp)

    def pipeline(self):
        "Creates a BloomdPipeline for pipelining multiple queries"
        return BloomdPipeline(self.conn, self.name, self.hash_keys)


class BloomdPipeline(object):
    "Provides an interface to a single Bloomd filter"
    def __init__(self, conn, name, hash_keys=False):
        """
        Creates a new BloomdPipeline object.

        :Parameters:
            - conn : The connection to use
            - name : The name of the filter
            - hash_keys : Should the keys be hashed client side
        """
        self.conn = conn
        self.name = name
        self.hash_keys = hash_keys
        self.buf = []

    def _get_key(self, key):
        """
        Returns the key we should send to the server
        """
        if self.hash_keys:
            return hashlib.sha1(key).hexdigest()
        return key

    def add(self, key):
        """
        Adds a new key to the filter. Returns True/False if the key was added.
        """
        self.buf.append(("add", "s %s %s" % (self.name, self._get_key(key))))
        return self

    def bulk(self, keys):
        "Performs a bulk set command, adds multiple keys in the filter"
        command = ("b %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        self.buf.append(("bulk", command))
        return self

    def drop(self):
        "Deletes the filter from the server. This is permanent"
        self.buf.append(("drop", "drop %s" % (self.name)))
        return self

    def close(self):
        """
        Closes the filter on the server.
        """
        self.buf.append(("close", "close %s" % (self.name)))
        return self

    def clear(self):
        """
        Clears the filter on the server.
        """
        self.buf.append(("clear", "clear %s" % (self.name)))
        return self

    def check(self, key):
        "Checks if the key is contained in the filter."
        self.buf.append(("check", "c %s %s" % (self.name, self._get_key(key))))
        return self

    def multi(self, keys):
        "Performs a multi command, checks for multiple keys in the filter"
        command = ("m %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        self.buf.append(("multi", command))
        return self

    def info(self):
        "Returns the info dictionary about the filter."
        self.buf.append(("info", "info %s" % (self.name)))
        return self

    def flush(self):
        "Forces the filter to flush to disk"
        self.buf.append(("flush", "flush %s" % (self.name)))
        return self

    def merge(self, pipeline):
        """
        Merges this pipeline with another pipeline. Commands from the
        other pipeline are appended to the commands of this pipeline.
        """
        self.buf.extend(pipeline.buf)
        return self

    def execute(self):
        """
        Executes the pipelined commands. All commands are sent to
        the server in the order issued, and responses are returned
        in appropriate order.
        """
        # Send each command
        buf = self.buf
        self.buf = []
        for name, cmd in buf:
            self.conn.send(cmd)

        # Get the responses
        all_resp = []
        for name, cmd in buf:
            if name in ("bulk", "multi"):
                resp = self.conn.read()
                if resp[:3] == "Yes" or resp[:2] == "No":
                    all_resp.append([r == "Yes" for r in resp.split(" ")])
                else:
                    all_resp.append(BloomdError("Got response: %s" % resp))

            elif name in ("add", "check"):
                resp = self.conn.read()
                if resp in ("Yes", "No"):
                    all_resp.append(resp == "Yes")
                else:
                    all_resp.append(BloomdError("Got response: %s" % resp))

            elif name in ("drop", "close", "clear", "flush"):
                resp = self.conn.read()
                if resp == "Done":
                    all_resp.append(True)
                else:
                    all_resp.append(BloomdError("Got response: %s" % resp))

            elif name == "info":
                try:
                    resp = self.conn.response_block_to_dict()
                    all_resp.append(resp)
                except BloomdError as e:
                    all_resp.append(e)
            else:
                raise Exception("Unknown command! Command: %s" % name)

        return all_resp
