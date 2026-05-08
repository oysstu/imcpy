import logging
import time
from operator import itemgetter
from typing import List, Tuple, Union

import imcpy
from imcpy.actors import IMCBase
from imcpy.common import multicast_ip
from imcpy.decorators import Periodic, Subscribe
from imcpy.exception import AmbiguousKeyError
from imcpy.network.udp import IMCSenderUDP
from imcpy.network.utils import get_interfaces

logger = logging.getLogger('imcpy.actors.dynamic')


class DynamicActor(IMCBase):
    """
    Actor which announces itself and maintains communication (heartbeat) with a set of specified nodes.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Set initial entities (services generated on first announce)
        self.entities = {'Daemon': 0, 'Service Announcer': 1}

        # IMC nodes to send heartbeat signal to (maintaining comms)
        self.heartbeat: List[Union[str, int, Tuple[int, str]]] = []

        # Override IMC services to announce (empty: will use default IMC+UDP services)
        self.services: List[str] = []

    def default_imc_services(self, ignore_local=True) -> List[str]:
        if self._port_imc:
            srv = ['imc+udp://{}:{}/'.format(adr[1], self._port_imc) for adr in get_interfaces(ignore_local)]
            if not srv:
                # No external interfaces available, announce localhost/loopback
                srv = ['imc+udp://{}:{}/'.format(adr[1], self._port_imc) for adr in get_interfaces(False)]
            return srv
        else:
            logger.debug('IMC socket not ready')
            return []

    @Subscribe(imcpy.EntityList)
    def _reply_entity_list(self, msg):
        """
        Respond to entity list queries
        """
        OpEnum = imcpy.EntityList.OperationEnum
        if msg.op == OpEnum.QUERY:
            try:
                node = self.resolve_node_id(msg)

                # Format entities into string and send back to node that requested it
                ent_lst_sorted = sorted(self.entities.items(), key=itemgetter(1))  # Sort by value (entity id)
                ent_lst = imcpy.EntityList()
                ent_lst.op = OpEnum.REPORT
                ent_lst.list = ';'.join('{}={}'.format(k, v) for k, v in ent_lst_sorted)
                self.send(node, ent_lst)
            except (AmbiguousKeyError, KeyError):
                logger.debug('Unable to resolve node when sending EntityList')

    @Periodic(30)
    def _query_entity_list(self):
        """
        Request entity list from nodes without one
        """
        for k, node in self._nodes.items():
            if not node.entities:
                q_ent = imcpy.EntityList()
                q_ent.op = imcpy.EntityList.OperationEnum.QUERY
                self.send(node, q_ent)

    @Periodic(10)
    def _send_announce(self):
        """
        Send an announce. Will use properties stored in this class (e.g self.lat, self.lon to set parameters)
        :return:
        """
        services = self.services if self.services else self.default_imc_services()
        self.announce.services = ';'.join(services)
        with IMCSenderUDP(multicast_ip, local_port=None, all_interfaces=True) as s:
            self.announce.set_timestamp_now()
            for i in range(30100, 30105):
                s.send(self.announce, i)

    @Periodic(1)
    def _send_heartbeat(self):
        """
        Send a heartbeat signal to nodes specified in self.heartbeat
        """
        hb = imcpy.Heartbeat()
        hb_sent = []
        for node_id in self.heartbeat:
            try:
                node = self.resolve_node_id(node_id)

                # Only send hb once if multiple keys resolve to same node
                if node not in hb_sent:
                    self.send(node, hb)
                    hb_sent.append(node)
            except AmbiguousKeyError as e:
                logger.exception(str(e) + '({})'.format(e.choices))
            except KeyError:
                pass
