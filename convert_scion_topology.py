#!/usr/bin/env python3
"""
Script to convert SCION topology from generated format to Kathara lab format.

The script automatically detects AS directories and maps them to node directories:
- ASff00_0_110 => AS_110
- ASff00_0_111 => AS_111
- etc.

For each AS:
- Copy certs/, crypto/, keys/ directories
- Consolidate all border routers into a single br.toml
- Rename cs*.toml to cs.toml
- Copy sd.toml and topology.json
- Update configuration files with proper IP addresses and unique port assignments
- Handle multiple links between border routers

Additionally generates Kathara lab configuration:
- lab.conf with node definitions and network connections
- .startup scripts for each node with IP configuration and SCION service startup
"""

import os
import shutil
from pathlib import Path
import re
import json
import toml


def extract_as_number(as_name):
    match = re.search(r'_(\d+)$', as_name)
    if match:
        return int(match.group(1))
    return None


def build_as_to_node_mapping(source_base):
    as_to_node = {}

    # Find all AS directories
    as_dirs = [d for d in source_base.iterdir()
               if d.is_dir() and d.name.startswith('AS')]

    # Sort by AS number for consistent ordering
    as_dirs_with_numbers = []
    for as_dir in as_dirs:
        as_num = extract_as_number(as_dir.name)
        if as_num is not None:
            as_dirs_with_numbers.append((as_num, as_dir.name))

    as_dirs_with_numbers.sort()

    # Create mapping: AS number with AS_ prefix (e.g., ASff00_0_110 -> AS_110)
    for as_num, as_name in as_dirs_with_numbers:
        as_to_node[as_name] = f"as_{as_num}"

    return as_to_node


def update_br_toml(file_path, node_number):
    config = toml.load(file_path)

    # Remove metrics section
    if 'metrics' in config:
        del config['metrics']

    # Update config_dir
    if 'general' in config:
        config['general']['config_dir'] = '/etc/scion/'
        # Set the border router ID to 'br' to match consolidated topology.json
        config['general']['id'] = 'br'

    # Update api address
    if 'api' in config:
        ip = node_to_ip(node_number)
        config['api']['addr'] = f'10.0.0.{ip}:31442'

    # Write back to file
    with open(file_path, 'w') as f:
        toml.dump(config, f)


def update_cs_toml(file_path, node_number):
    config = toml.load(file_path)

    # Remove metrics section
    if 'metrics' in config:
        del config['metrics']

    # Remove tracing section
    if 'tracing' in config:
        del config['tracing']

    # Update config_dir
    if 'general' in config:
        config['general']['config_dir'] = '/etc/scion/'

    # Update database paths
    if 'trust_db' in config:
        config['trust_db']['connection'] = '/etc/scion/trust.db'
    if 'beacon_db' in config:
        config['beacon_db']['connection'] = '/etc/scion/beacon.db'
    if 'path_db' in config:
        config['path_db']['connection'] = '/etc/scion/path.db'

    # Update api address
    if 'api' in config:
        ip = node_to_ip(node_number)
        config['api']['addr'] = f'10.0.0.{ip}:31152'

    # Write back to file
    with open(file_path, 'w') as f:
        toml.dump(config, f)


def update_sd_toml(file_path, node_number):
    """
    Update sd.toml file:
    1. Remove metrics section
    2. Remove tracing section
    3. Change all addresses to 10.0.0.NODE_NUMBER
    4. Change folder for databases to /etc/scion/
    5. Change config_dir to /etc/scion/
    """
    config = toml.load(file_path)

    # Remove metrics section
    if 'metrics' in config:
        del config['metrics']

    # Remove tracing section
    if 'tracing' in config:
        del config['tracing']

    # Update config_dir
    if 'general' in config:
        config['general']['config_dir'] = '/etc/scion/'

    # Update database paths
    if 'trust_db' in config:
        config['trust_db']['connection'] = '/etc/scion/trust.db'
    if 'path_db' in config:
        config['path_db']['connection'] = '/etc/scion/path.db'

    # Update sd address
    if 'sd' in config:
        ip = node_to_ip(node_number)
        config['sd']['address'] = f'10.0.0.{ip}:30255'

    # Update api address
    if 'api' in config:
        ip = node_to_ip(node_number)
        config['api']['addr'] = f'10.0.0.{ip}:30955'

    # Write back to file
    with open(file_path, 'w') as f:
        toml.dump(config, f)


def extract_node_from_isd_as(isd_as):
    """
    Extract AS number from ISD-AS format.
    Examples:
      1-ff00:0:110 -> 110
      1-ff00:0:111 -> 111
    """
    # Extract the last part after the last colon
    match = re.search(r':(\d+)$', isd_as)
    if match:
        as_number = int(match.group(1))
        return as_number
    return None


def node_to_ip(node_number):
    """
    Convert AS number to IP address.
    AS 110 -> 10.0.0.110, AS 111 -> 10.0.0.111, etc.
    """
    return node_number


class PortAllocator:
    """
    Manages port allocation for border router interfaces.
    Ensures unique ports are assigned to each link between nodes.
    """
    def __init__(self, base_port=50000):
        self.base_port = base_port
        self.next_port = base_port
        # Key: (smaller_node, larger_node, link_index), Value: port
        self.port_assignments = {}

    def get_port(self, node_a, node_b, link_index=0):
        """
        Get a unique port for a specific link between two nodes.

        Args:
            node_a: First node number
            node_b: Second node number
            link_index: Index for multiple links between same nodes (0, 1, 2, ...)

        Returns:
            Unique port number for this specific link
        """
        # Create canonical key (sorted order for consistency)
        smaller = min(node_a, node_b)
        larger = max(node_a, node_b)
        key = (smaller, larger, link_index)

        # Return existing port if already assigned
        if key in self.port_assignments:
            return self.port_assignments[key]

        # Assign new port
        port = self.next_port
        self.port_assignments[key] = port
        self.next_port += 1

        return port


def update_topology_json(file_path, node_number, port_allocator):
    """
    Update topology.json file with proper IP addresses and port assignments.

    Args:
        file_path: Path to topology.json file
        node_number: Node number for this AS
        port_allocator: PortAllocator instance for managing port assignments
    """
    with open(file_path, 'r') as f:
        topology = json.load(f)
    if 'test_dispatcher' in topology:
        del(topology['test_dispatcher'])

    # Update control_service addresses
    if 'control_service' in topology:
        for service_name, service_data in topology['control_service'].items():
            if 'addr' in service_data:
                # Extract port from existing address
                port = service_data['addr'].split(':')[1]
                ip = node_to_ip(node_number)
                service_data['addr'] = f'10.0.0.{ip}:{port}'

    # Update discovery_service addresses
    if 'discovery_service' in topology:
        for service_name, service_data in topology['discovery_service'].items():
            if 'addr' in service_data:
                # Extract port from existing address
                port = service_data['addr'].split(':')[1]
                ip = node_to_ip(node_number)
                service_data['addr'] = f'10.0.0.{ip}:{port}'

    # Update border_routers addresses and consolidate into a single border router
    if 'border_routers' in topology:
        # Track link indices for multiple connections between same node pairs
        # Key: (smaller_node, larger_node), Value: current link index
        link_counters = {}

        # Collect all interfaces from all border routers
        all_interfaces = {}
        consolidated_internal_addr = None

        for br_name, br_data in topology['border_routers'].items():
            # Keep the first internal_addr we find (or use the last one)
            if 'internal_addr' in br_data:
                # Extract port from existing address
                port = br_data['internal_addr'].split(':')[1]
                ip = node_to_ip(node_number)
                consolidated_internal_addr = f'10.0.0.{ip}:{port}'

            # Collect all interfaces from this border router
            if 'interfaces' in br_data:
                for interface_id, interface_data in br_data['interfaces'].items():
                    # Copy the interface data
                    all_interfaces[interface_id] = interface_data

        # Now process all collected interfaces
        for interface_id, interface_data in all_interfaces.items():
            if 'underlay' in interface_data:
                underlay = interface_data['underlay']

                # Get remote node for port calculation
                remote_node = None
                if 'isd_as' in interface_data:
                    remote_isd_as = interface_data['isd_as']
                    remote_node = extract_node_from_isd_as(remote_isd_as)

                if remote_node is not None:
                    # Create a canonical key for the node pair (sorted order)
                    node_pair = (min(node_number, remote_node), max(node_number, remote_node))

                    # Get the link index for this pair (or 0 if first link)
                    link_index = link_counters.get(node_pair, 0)

                    # Increment counter for next potential link between same nodes
                    link_counters[node_pair] = link_index + 1

                    # Get a unique port for this connection with link index
                    connection_port = port_allocator.get_port(node_number, remote_node, link_index)

                    # Update local address
                    if 'local' in underlay:
                        ip = node_to_ip(node_number)
                        underlay['local'] = f'10.0.0.{ip}:{connection_port}'

                    # Update remote address (uses same port as it's the same connection)
                    if 'remote' in underlay:
                        remote_ip = node_to_ip(remote_node)
                        underlay['remote'] = f'10.0.0.{remote_ip}:{connection_port}'

        # Replace all border routers with a single consolidated one
        topology['border_routers'] = {
            'br': {
                'internal_addr': consolidated_internal_addr,
                'interfaces': all_interfaces
            }
        }

    # Write back to file
    with open(file_path, 'w') as f:
        json.dump(topology, f, indent=2)
        f.write('\n')  # Add trailing newline


def generate_kathara_configs(dest_base, as_to_node):
    """
    Generate Kathara lab.conf and startup scripts for all nodes.

    Args:
        dest_base: Base directory for the Kathara lab
        as_to_node: Dictionary mapping AS names to node names
    """
    # Start building lab.conf content
    labfile = """LAB_DESCRIPTION="SCION single collision domain topology"
LAB_VERSION=1.0
LAB_AUTHOR="Network Security Group, ETH Zurich"
LAB_WEB="https://netsec.ethz.ch"
"""

    # Sort nodes by AS number for consistent ordering
    sorted_nodes = []
    for as_name, node_name in as_to_node.items():
        as_num = extract_as_number(as_name)
        sorted_nodes.append((as_num, node_name))
    sorted_nodes.sort()

    # Generate configuration for each node
    for node_idx, (as_num, node_name) in enumerate(sorted_nodes):
        ip = node_to_ip(as_num)

        # Add to lab.conf
        labfile += f"""
# Config for {node_name}
{node_name}[0]=net_0
{node_name}[1]=net_1
{node_name}[image]="kathara/scion-local"
"""

        # Generate startup script
        startup_script = f"""# === Startup Script for {node_name} ===

ip address add 10.0.0.{ip}/24 dev eth0
ip address add 192.168.0.{ip}/24 dev eth1

# Start SCION services
systemctl start scion-dispatcher.service
systemctl start scion-router.service
systemctl start scion-control.service
systemctl start scion-daemon.service
systemctl status scion-*.service

"""
        # Write startup script
        startup_path = dest_base / f"{node_name}.startup"
        with open(startup_path, "w") as fd:
            fd.write(startup_script)
        print(f"  Generated {node_name}.startup")

    # Write lab.conf
    lab_conf_path = dest_base / "lab.conf"
    with open(lab_conf_path, "w") as fd:
        fd.write(labfile)
    print(f"\n✓ Generated lab.conf")


def main():
    script_dir = Path(__file__).parent
    source_base = script_dir / "input_scion" / "gen"
    dest_base = script_dir / "KatharaLab"

    # Check if source exists
    if not source_base.exists():
        print(f"Error: Source directory {source_base} does not exist!")
        return

    # Create destination base if it doesn't exist
    dest_base.mkdir(parents=True, exist_ok=True)

    # Automatically build AS to node mapping
    as_to_node = build_as_to_node_mapping(source_base)

    if not as_to_node:
        print("Error: No AS directories found in source!")
        return

    print(f"Found {len(as_to_node)} AS directories:")
    for as_name, node_name in sorted(as_to_node.items()):
        print(f"  {as_name} => {node_name}")
    print()

    # Create a shared port allocator for all nodes
    port_allocator = PortAllocator(base_port=50000)

    # Process each AS
    for as_name, node_name in as_to_node.items():
        as_dir = source_base / as_name
        node_dir = dest_base / node_name / "etc" / "scion"

        if not as_dir.exists():
            print(f"Warning: {as_name} directory not found, skipping...")
            continue

        # Extract AS number from node_name (e.g., "AS_110" -> 110)
        node_number = int(node_name.replace('as_', ''))
        ip = node_to_ip(node_number)

        print(f"Processing {as_name} => {node_name} (10.0.0.{ip})")

        # Create node directory
        node_dir.mkdir(parents=True, exist_ok=True)

        # Copy directories: certs, crypto, keys
        for dir_name in ["certs", "crypto", "keys"]:
            src_dir = as_dir / dir_name
            dst_dir = node_dir  / dir_name

            if src_dir.exists():
                if dst_dir.exists():
                    shutil.rmtree(dst_dir)
                shutil.copytree(src_dir, dst_dir)
                print(f"  Copied {dir_name}/")
            else:
                print(f"  Warning: {dir_name}/ not found in {as_name}")

        # Find and rename br*.toml to br.toml
        br_files = list(as_dir.glob("br*.toml"))
        if br_files:
            br_file = br_files[0]
            shutil.copy2(br_file, node_dir / "br.toml")
            print(f"  Copied and renamed {br_file.name} => br.toml")
            # Update br.toml configuration
            update_br_toml(node_dir / "br.toml", node_number)
            print(f"  Updated br.toml configuration")
        else:
            print(f"  Warning: No br*.toml file found in {as_name}")

        # Find and rename cs*.toml to cs.toml
        cs_files = list(as_dir.glob("cs*.toml"))
        if cs_files:
            cs_file = cs_files[0]
            shutil.copy2(cs_file, node_dir / "cs.toml")
            print(f"  Copied and renamed {cs_file.name} => cs.toml")
            # Update cs.toml configuration
            update_cs_toml(node_dir / "cs.toml", node_number)
            print(f"  Updated cs.toml configuration")
        else:
            print(f"  Warning: No cs*.toml file found in {as_name}")

        # Copy sd.toml
        sd_file = as_dir / "sd.toml"
        if sd_file.exists():
            shutil.copy2(sd_file, node_dir / "sd.toml")
            print(f"  Copied sd.toml")
            # Update sd.toml configuration
            update_sd_toml(node_dir / "sd.toml", node_number)
            print(f"  Updated sd.toml configuration")
        else:
            print(f"  Warning: sd.toml not found in {as_name}")

        # Copy topology.json
        topology_file = as_dir / "topology.json"
        if topology_file.exists():
            shutil.copy2(topology_file, node_dir / "topology.json")
            print(f"  Copied topology.json")
            # Update topology.json configuration
            update_topology_json(node_dir / "topology.json", node_number, port_allocator)
            print(f"  Updated topology.json configuration")
        else:
            print(f"  Warning: topology.json not found in {as_name}")

    print("\n✓ Reorganization complete!")

    # Generate Kathara configuration files
    print("\nGenerating Kathara configuration files...")
    generate_kathara_configs(dest_base, as_to_node)

    print("\n✓ All done! Kathara lab is ready.")


if __name__ == "__main__":
    main()
