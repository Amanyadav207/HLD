import hashlib
from typing import List, Dict
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """
        Initialize the consistent hash ring
        
        Args:
            nodes: List of node identifiers (parsed from comma-separated string)
            virtual_nodes: Number of virtual nodes per physical node
        """
        self.virtual_nodes = virtual_nodes
        self.hash_ring: Dict[int, str] = {}  # Mapping of hash to physical node
        self.sorted_keys: List[int] = []  # Sorted list of hash values

        # Add nodes to the hash ring
        for node in nodes:
            self.add_node(node)

    def _hash(self, key: str) -> int:
        """
        Generate a hash for the given key using MD5
        
        Args:
            key: The key to hash
        
        Returns:
            A consistent hash value (integer)
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str) -> None:
        """
        Add a new node to the hash ring
        
        Args:
            node: Node identifier to add
        """
        for i in range(self.virtual_nodes):
            virtual_node_key = f"{node}#{i}"
            hash_value = self._hash(virtual_node_key)
            self.hash_ring[hash_value] = node
            self.sorted_keys.append(hash_value)

        self.sorted_keys.sort()

    def remove_node(self, node: str) -> None:
        """
        Remove a node from the hash ring
        
        Args:
            node: Node identifier to remove
        """
        keys_to_remove = [key for key, value in self.hash_ring.items() if value == node]
        for key in keys_to_remove:
            del self.hash_ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, key: str) -> str:
        """
        Get the node responsible for the given key
        
        Args:
            key: The key to look up
            
        Returns:
            The node responsible for the key
        """
        if not self.sorted_keys:
            raise ValueError("No nodes available in the hash ring.")

        hash_value = self._hash(key)
        index = bisect(self.sorted_keys, hash_value)

        # If index is equal to the length, wrap around to the first node
        if index == len(self.sorted_keys):
            index = 0

        return self.hash_ring[self.sorted_keys[index]]