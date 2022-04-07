import ifaddr
from typing import List, Tuple

def get_interfaces(ignore_local=True, only_ipv4=True) -> List[Tuple[str, str, int]]:
    ifaces = []
    for adapter in ifaddr.get_adapters():
        if adapter.name != 'lo' or not ignore_local:
            for ip in adapter.ips:
                if type(ip.ip) is str or not only_ipv4:
                    ifaces.append((adapter.name, ip.ip, ip.network_prefix))
    
    return ifaces


if __name__ == '__main__':
    pass

    