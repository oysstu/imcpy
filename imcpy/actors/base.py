import asyncio
import datetime
import inspect
import logging
import os
import socket
import sys
import tempfile
import time
import types
from contextlib import suppress
from typing import Dict, List, Optional, Tuple, Type, Union

import imcpy
from imcpy.decorators import *
from imcpy.exception import AmbiguousKeyError
from imcpy.network.tcp import IMCProtocolTCPServer
from imcpy.network.udp import IMCProtocolUDP, IMCSenderUDP, get_imc_socket, get_multicast_socket
from imcpy.node import IMCNode, IMCService

logger = logging.getLogger('imcpy.actors.base')


class IMCBase:
    """
    Base class for IMC communications.
    Implements an event loop, subscriptions, IMC node bookkeeping
    """

    def __init__(
        self,
        imc_id: int = 0x3334,
        static_port: Optional[int] = None,
        backseat_port: Optional[int] = None,
        verbose_nodes=False,
        log_enable=False,
        log_root: Optional[str] = None,
    ):
        """
        Initialize the IMC comms. Does not start the event loop until run() is called
        :param imc_id: The IMC id this node should operate under
        :param static_port: Optional static port to listen for IMC messages (useful if DUNE uses static transports)
        :param backseat_port: Listen for TCP+IMC connections from backseat on this port (similar to DUNE backseat).
        :param verbose_nodes: If true, the connected nodes are printed out every 10 seconds
        :param log_enable: Enable logging of incoming and outgoing IMC messages (.lsf)
        :param log_root: Root directory for IMC logs (default: /tmp/, or equivalent)
        """
        # Arguments
        self.imc_id = imc_id
        self.static_port = static_port
        self.backseat_port: Optional[int] = backseat_port
        self.verbose_nodes = verbose_nodes
        self.log_enable = log_enable
        self.log_root = os.path.join(tempfile.gettempdir(), 'imcpy') if log_root is None else log_root

        # Overridden/updated in subclasses
        self.announce = imcpy.Announce()
        self.announce.src = self.imc_id
        self.announce.sys_name = f'ccu-imcpy-{socket.gethostname().lower()}'
        self.announce.sys_type = imcpy.SystemType.CCU
        self.announce.owner = 0xFFFF
        self.announce.src_ent = 1
        self.entities = {'Daemon': 0}
        self.services: List[str] = []

        # Asyncio loop, tasks, and callbacks
        self._loop: Optional[asyncio.BaseEventLoop] = None
        self._task_mc: Optional[asyncio.Task] = None
        self._task_imc: Optional[asyncio.Task] = None
        self._task_server: Optional[asyncio.Task] = None
        self._backseat_server: Optional[IMCProtocolTCPServer] = None
        self._subs: Dict[Type[imcpy.Message], List[types.MethodType]] = {}

        # IMC/Multicast ports (assigned when socket is created)
        self._port_imc: Optional[int] = None
        self._port_mc: Optional[int] = None

        # Using a map from (imc address, sys_name) to a node instance
        self._nodes: Dict[Tuple[int, str], IMCNode] = {}

        # Static transports
        # Adding imcpy.Message transports all messages
        # TODO: add client API for adding and removing static transports
        self._static_transports: Dict[Type[imcpy.Message], List[IMCService]] = {}

        # Runtime data
        self.t_start = None
        self.log_dir = None  # Log directory (subdirectory of log_root)
        self.log_imc_fh = None  # File handle (IMC)
        self.log_console_fh = None  # File handle (console/logging)
        self.log_level = logging.DEBUG

    #
    # Private
    #

    def _log_start(self):
        # Setup logging
        dt = datetime.datetime.today()
        dt_datestr = dt.strftime('%Y%m%d')
        dt_timestr = dt.strftime('%H%M%S')
        self.log_dir = os.path.join(self.log_root, self.announce.sys_name, dt_datestr, dt_timestr)
        os.makedirs(self.log_dir, exist_ok=True)

        # Console log
        self.log_console_fh = logging.FileHandler(os.path.join(self.log_dir, 'Output.txt'))
        self.log_console_fh.setLevel(self.log_level)
        fmt = logging.Formatter(fmt='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')
        self.log_console_fh.setFormatter(fmt)
        logging.getLogger('').addHandler(self.log_console_fh)  # Add to root logger

        # IMC message log
        logger.info(f'Starting file log ({self.log_dir})')
        self.log_imc_fh = open(os.path.join(self.log_dir, 'Data.lsf'), 'wb')
        log_ctl = imcpy.LoggingControl()
        log_ctl.set_timestamp_now()
        log_ctl.src = self.announce.src
        log_ctl.op = imcpy.LoggingControl.ControlOperationEnum.STARTED
        log_ctl.name = dt_datestr + '/' + dt_timestr
        self.log_imc_fh.write(log_ctl.serialize())

    def _log_stop(self):
        if self.log_imc_fh and not self.log_imc_fh.closed:
            logger.info(f'Stopping file log ({self.log_dir})')
            dt = datetime.datetime.today()
            log_ctl = imcpy.LoggingControl()
            log_ctl.set_timestamp_now()
            log_ctl.src = self.announce.src
            log_ctl.op = imcpy.LoggingControl.ControlOperationEnum.STOPPED
            log_ctl.name = dt.strftime('%Y%m%d') + '/' + dt.strftime('%H%M%S')
            self.log_imc_fh.write(log_ctl.serialize())
            self.log_imc_fh.close()

        if self.log_console_fh:
            self.log_console_fh.close()

    #
    # Events
    #

    def on_connect(self, node_id):
        """
        Called when a new node is added to the node map
        :param node_id: The id of the node Tuple[imc_address, imc_name]
        """
        raise NotImplementedError('Abstract implementation')

    def on_disconnect(self, node_id):
        """
        Called when a new node is removed to the node map (timeout)
        :param node_id: The id of the node Tuple[imc_address, imc_name]
        """
        raise NotImplementedError('Abstract implementation')

    def on_first_heartbeat(self, node_id):
        """
        Called when a new node sends its first heartbeat message (transports started)
        :param node_id: The id of the node Tuple[imc_address, imc_name]
        """
        raise NotImplementedError('Abstract implementation')

    #
    # Callable functions
    #

    def stop(self):
        """
        Cancels all running tasks and stops the event loop.
        :return:
        """
        logger.info('Stop called by user. Cancelling all running tasks.')
        loop = self._loop

        if loop is not None:
            for task in asyncio.all_tasks(loop):
                task.cancel()

            async def exit_event_loop():
                logger.info('Tasks cancelled. Stopping event loop.')
                loop.stop()

            asyncio.ensure_future(exit_event_loop())

    def run(self):
        """
        Starts the event loop.
        """
        self.t_start = time.time()

        if self.log_enable:
            self._log_start()

        # Run setup if it hasn't been done yet
        if not self._loop:
            self._setup_event_loop()

        # Start event loop
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
                # Now we should await task to execute it's cancellation.
                # Cancelled task raises asyncio.CancelledError that we can suppress when canceling
                with suppress(asyncio.CancelledError):
                    self._loop.run_until_complete(task)
        finally:
            self._loop.close()

            # Finish IMC log
            if self.log_enable:
                self._log_stop()

    def post_message(self, msg: imcpy.Message):
        """
        Post a message to the subscribed functions
        :param msg: The IMC message to post
        :return:
        """
        # Check that message is subclass of imcpy.Message
        # Note: messages that exists in DUNE, but has no pybind11 bindings are returned as imcpy.Message
        class_hierarchy = inspect.getmro(type(msg))
        if imcpy.Message in class_hierarchy:
            # Post message of known type
            if type(msg) is not imcpy.Message:
                if type(msg) in self._subs:
                    for fn in self._subs[type(msg)]:
                        try:
                            fn(msg)
                        except Exception as e:
                            self.on_exception(loc=fn.__qualname__, exc=e)
            else:
                # Emit warning on IMC type without bindings
                logger.warning(f'Unknown IMC message received: {msg.msg_name} ({msg.msg_id}) from {msg.src}')

            # Post messages to functions subscribed to all messages (imcpy.Message)
            if imcpy.Message in self._subs:
                for fn in self._subs[imcpy.Message]:
                    try:
                        fn(msg)
                    except Exception as e:
                        self.on_exception(loc=fn.__qualname__, exc=e)
        else:
            logger.warning(f'Received message that is not subclass of imcpy.Message: {type(msg)}')

    def resolve_node_id(self, node_id: Union[int, str, Tuple[int, str], imcpy.Message, IMCNode]) -> IMCNode:
        """
        This function searches the map of connected nodes and returns a match (if unique)

        This function can throw the following exceptions
        KeyError: Node not found (not connected)
        AmbiguousKeyError: Multiple nodes matches the id (e.g multiple nodes announcing the same name)
        ValueError: Id parameter has an unexpected type
        :param node_id: Can be one of the following: imcid(int), imcname(str), node(tuple(int, str)), imcpy.message
        :return: An instance of the IMCNode class
        """

        # Resolve IMCNode
        if type(node_id) is str or type(node_id) is int:
            # Type int or str: either imc name or imc id
            # Search for keys with the name or id, raise exception if not found or ambiguous
            idx = 0 if type(node_id) is int else 1
            possible_nodes = [x for x in self._nodes.keys() if x[idx] == node_id]
            if not possible_nodes:
                raise KeyError('Specified IMC node does not exist.')
            elif len(possible_nodes) > 1:
                raise AmbiguousKeyError('Specified IMC node has multiple possible choices', choices=possible_nodes)
            else:
                return self._nodes[possible_nodes[0]]
        elif type(node_id) is tuple:
            # Type Tuple(int, str): unique identifier of both imc id and name
            # Determine the correct order of arguments
            if type(node_id[0]) is int and type(node_id[1]) is str:
                return self._nodes[node_id]
            else:
                raise TypeError('Node id tuple must be (int, str).')
        elif type(node_id) is IMCNode:
            # Resolve by an preexisting IMCNode object
            return self.resolve_node_id((node_id.src, node_id.sys_name))
        elif isinstance(node_id, imcpy.Message):
            # Resolve by imc address in received message (equivalent to imc id)
            return self.resolve_node_id(node_id.src)
        else:
            raise TypeError(f'Expected node_id as int, str, tuple(int,str) or Message, received {type(node_id)}')

    def add_node(self, node: IMCNode):
        """
        Add an IMC node to the map.
        :param node: The node to be added to the map. The src and sys_name properties must be set
        """
        self._nodes[(node.src, node.sys_name)] = node

    def remove_node(self, key):
        """
        Remove an IMC node from the map.
        :param key: One of the supported key formats in resolve_node_id
        """
        node = self.resolve_node_id(key)
        del self._nodes[(node.src, node.sys_name)]

    def add_static_transport(self, imc_service: IMCService, msg_types: List[Type[imcpy.Message]]):
        """
        Setup an outgoing static transport of the imc message types to the target imc service.
        :param imc_service: The destination imc service (must contain imc+udp)
        :param msg_types: A list of message types to transport
        :return:
        """
        for msg_type in msg_types:
            self._static_transports[msg_type].append(imc_service)

    def send_static(self, msg, set_timestamp=True):
        """
        Send a message to the static destinations declared in static_transports
        :param msg: The message to be sendt (note: dst is not filled automatically)
        :param set_timestamp: If true, sets the timestamp to current time
        """
        # Fill out source params
        msg.src = self.imc_id

        if set_timestamp:
            msg.set_timestamp_now()

        for svc in self._static_transports.get(type(msg), []):
            with IMCSenderUDP(svc.ip) as s:
                s.send(message=msg, port=svc.port)
        for svc in self._static_transports.get(imcpy.Message, []):
            with IMCSenderUDP(svc.ip) as s:
                s.send(message=msg, port=svc.port)

    def send_backseat(self, msg, set_timestamp=True):
        # Send to connected backseat (if any)
        if self._backseat_server is not None and self._loop is not None:
            asyncio.ensure_future(self._backseat_server.write_message(msg), loop=self._loop)

    def send(self, node_id, msg, set_timestamp=True, ignore_local=True):
        """
        Send an imc message to the specified imc node. The node can be specified through it's imc address, system name
        or a tuple of both. If either of the first two does not uniquely specify a node an AmbiguousNode exception is
        raised.
        :param node_id: The destination node (imc adr (int), system name (str) or a tuple(imc_adr, sys_name))
        :param msg: The imc message to send
        :param set_timestamp: Set the timestamp to current system time
        """

        # Fill out source params
        msg.src = self.imc_id

        if set_timestamp:
            msg.set_timestamp_now()

        node = self.resolve_node_id(node_id)
        node.send(msg, log_fh=self.log_imc_fh, ignore_local=ignore_local)

        # Send to static destinations
        self.send_static(msg)

        # Send to connected backseat (if any)
        self.send_backseat(msg)

    def on_exception(self, loc, exc):
        """
        Can be overridden in subclasses to handle uncaught exceptions in @Subscribe, @Periodic, @RunOnce functions
        :return:
        """
        logger.error(f'Uncaught exception ({type(exc).__qualname__}) in {loc}: {exc}')

    #
    # Private
    #

    def _start_subscriptions(self):
        """
        Add asyncio datagram endpoint for all subscriptions
        """
        assert self._loop is not None, 'Event loop not initialized'

        # Add datagram endpoint for multicast announce
        mc_sock = get_multicast_socket()
        self._port_mc = mc_sock.getsockname()[1]
        multicast_listener = self._loop.create_datagram_endpoint(lambda: IMCProtocolUDP(self), sock=mc_sock)
        self._task_mc = asyncio.ensure_future(multicast_listener, loop=self._loop)

        # Add datagram endpoint for UDP IMC messages
        imc_sock = get_imc_socket(static_port=self.static_port)
        self._port_imc = imc_sock.getsockname()[1]
        imc_listener = self._loop.create_datagram_endpoint(lambda: IMCProtocolUDP(self), sock=imc_sock)
        self._task_imc = asyncio.ensure_future(imc_listener, loop=self._loop)

        # Add backseat TCP server if port is set
        if self.backseat_port is not None:
            self._backseat_server = IMCProtocolTCPServer(self)
            logger.info(f'Setting up backseat server on 127.0.0.1:{self.backseat_port}')
            backseat_co = asyncio.start_server(self._backseat_server.on_connection, '127.0.0.1', self.backseat_port)
            self._task_backseat = asyncio.ensure_future(backseat_co, loop=self._loop)

    def _setup_event_loop(self):
        """
        Setup of event loop and decorated functions
        """
        # Add event loop to instance
        if not self._loop:
            self._loop = asyncio.get_event_loop()

        decorated = [(name, method) for name, method in inspect.getmembers(self) if hasattr(method, '_decorators')]
        for name, method in decorated:
            for decorator in method._decorators:
                decorator.add_event(self._loop, self, method)

                if type(decorator) is Subscribe:
                    # Collect subscribed message types for each function
                    for msg_type in decorator.subs:
                        try:
                            self._subs[msg_type].append(method)
                        except (KeyError, AttributeError):
                            self._subs[msg_type] = [method]

        # Sort subscriptions by position in inheritance hierarchy (parent classes are called first)
        cls_hier = [x.__qualname__ for x in inspect.getmro(type(self))]
        for msg_type, methods in self._subs.items():
            methods.sort(key=lambda x: -cls_hier.index(x.__qualname__.split('.')[0]))

        # Subscriptions has been collected from all decorators
        # Add asyncio datagram endpoints to event loop
        self._start_subscriptions()

    @Periodic(65)
    def _prune_nodes(self):
        """
        Clear nodes that have not announced themselves or sent heartbeat in the past 60 seconds
        """
        t = time.time()
        rm_keys = []  # Avoid changes to dict during iteration
        for key, node in self._nodes.items():
            has_heartbeat = type(node.t_last_heartbeat) is float and (t - node.t_last_heartbeat) < 60
            has_announce = node.t_last_announce is not None and (t - node.t_last_announce) < 60
            if not (has_heartbeat or has_announce) and not node.is_fixed:
                logger.info(f'Connection to node "{node}" timed out')
                rm_keys.append(key)

        for key in rm_keys:
            try:
                self.remove_node(key)
                try:
                    self.on_disconnect(key)
                except NotImplementedError:
                    pass
            except (KeyError, AttributeError) as e:
                logger.exception(f'Encountered exception when removing node ({e.msg})')

    @Periodic(10)
    def _print_connected_nodes(self):
        if self.verbose_nodes:
            logger.debug(f'Connected nodes: {list(self._nodes.keys())}')

    @Subscribe(imcpy.Announce)
    def _recv_announce(self, msg):
        # TODO: Check if IP of a node changes

        # Return if announce originates from this ccu or on duplicate IMC id
        if self.announce and msg.src == self.announce.src:
            # Is another system broadcasting our IMC id?
            if msg.sys_name != self.announce.sys_name:
                logger.warning(f'Another system is announcing the same IMC id ({msg.sys_name})')
            return

        # Update announce
        key = (msg.src, msg.sys_name)
        try:
            self._nodes[key].update_announce(msg)
        except KeyError:
            # If the key is new, check for duplicate names/imc addresses
            key_imcadr = [x for x in self._nodes.keys() if x[0] == key[0] or x[1] == key[1]]
            if key_imcadr:
                logger.warning(f'Multiple nodes are announcing the same IMC address or name: {key} and {key_imcadr}')

            # New node
            self.add_node(IMCNode.from_announce(msg))

            try:
                self.on_connect(key)
            except NotImplementedError:
                pass

    @Subscribe(imcpy.Heartbeat)
    def _recv_heartbeat(self, msg):
        try:
            node = self.resolve_node_id(msg)
            first_heartbeat = node.t_last_heartbeat is None
            node.update_heartbeat()

            if first_heartbeat:
                try:
                    self.on_first_heartbeat((node.src, node.sys_name))
                except NotImplementedError:
                    pass
        except (AmbiguousKeyError, KeyError):
            logger.debug(f'Received heartbeat from unannounced node ({msg.src})')

    @Subscribe(imcpy.EntityList)
    def _recv_entity_list(self, msg):
        """
        Process received entity lists
        """
        OpEnum = imcpy.EntityList.OperationEnum
        if msg.op == OpEnum.REPORT:
            try:
                node = self.resolve_node_id(msg)
                node.update_entity_list(msg)
            except (AmbiguousKeyError, KeyError):
                logger.debug('Unable to resolve node when updating EntityList')

    @Subscribe(imcpy.EntityInfo)
    def _recv_entity_info(self, msg: imcpy.EntityInfo):
        """
        Process entity info messages. Mostly for systems that does not announce EntityList
        """
        try:
            node = self.resolve_node_id(msg)
            node.update_entity_id(ent_id=msg.src_ent, ent_label=msg.label)
        except (AmbiguousKeyError, KeyError):
            pass


if __name__ == '__main__':
    pass
