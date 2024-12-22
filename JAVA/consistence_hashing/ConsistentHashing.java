package consistence_hashing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ConsistentHashing {

    // TreeMap to represent the hash ring
    private final TreeMap<Integer, String> hashRing = new TreeMap<>();
    private final Map<String, List<String>> serverToKeys = new HashMap<>();
    private final int virtualNodes; //number of virtual nodes per server

    public ConsistentHashing(int virtualNodes) {
        this.virtualNodes = virtualNodes;
    }

    // Hash function to convert input into a hash value
    private int hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
            return Math.abs((digest[0] & 0xFF) << 24 | (digest[1] & 0xFF) << 16 | (digest[2] & 0xFF) << 8 | (digest[3] & 0xFF));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    // Add a server to the hash ring (including virtual nodes)
    public void addServer(String server) {
        serverToKeys.putIfAbsent(server, new ArrayList<>()); // Initialize key list for the server
        for(int i=0; i<virtualNodes; i++) {
            String virtualNodeName = server + "_VN_" +i;
            int hash = hash(virtualNodeName);
            hashRing.put(hash, server);
            System.out.println("Added virtual node: " + virtualNodeName + " with hash: " + hash);
        }

        // Reassign keys impacted by this addition
        System.out.println("Reassigning keys for new server " + server + "...");
        reassignKeysAfterServerAddition(server);
    }

    public void removeServer(String server) {
        for(int i=0; i<virtualNodes; i++) {
            String virtualNodeName = server + "_VN_" +i;
            int hash = hash(virtualNodeName);
            hashRing.remove(hash);
            System.out.println("Removed virtual node: " + virtualNodeName + " with hash: " + hash);
        }

        // Remove keys associated with this server
        System.out.println("Reassigning keys from " + server + "...");
        reassignKeysAfterServerRemoval(server);
    }

    // Get the server responsible for a key
    public String getServer(String key) {
        int hash = hash(key);
        // Find the closest server using ceilingEntry
        Map.Entry<Integer, String> entry = hashRing.ceilingEntry(hash);
        if(entry == null) {
            entry = hashRing.firstEntry();
        }
        return entry.getValue();
    }

    // Assign a key to a server and store it in the server-to-keys map
    public void assignKey(String key) {
        String server = getServer(key);
        serverToKeys.get(server).add(key);
        System.out.println("Assigned key " + key + " to server " + server);
    }

    // Reassign keys impacted by the addition of a new server
    private void reassignKeysAfterServerAddition(String newServer) {
        // Iterate over the virtual nodes of the new server
        for (int i = 0; i < virtualNodes; i++) {
            String virtualNodeName = newServer + "_VN_" + i;
            int virtualNodeHash = hash(virtualNodeName);

            // Find the next server in the hash ring
            Integer nextServerHash = hashRing.ceilingKey(virtualNodeHash);
            if (nextServerHash == null) {
                // Wrap around to the first server if no server exists after this hash
                nextServerHash = hashRing.firstKey();
            }
            String nextServer = hashRing.get(nextServerHash);

            if (nextServer != null && !nextServer.equals(newServer)) {
                // Get keys assigned to the next server
                List<String> nextServerKeys = serverToKeys.get(nextServer);
                if (nextServerKeys != null) {
                    // Reassign keys now belonging to the new server
                    Iterator<String> keyIterator = nextServerKeys.iterator();
                    while (keyIterator.hasNext()) {
                        String key = keyIterator.next();
                        int keyHash = hash(key);

                        // Check if the key now belongs to the new server
                        if (keyHash <= virtualNodeHash || (nextServerHash != null && virtualNodeHash < nextServerHash)) {
                            serverToKeys.get(newServer).add(key);
                            keyIterator.remove();
                            System.out.println("Key " + key + " reassigned from " + nextServer + " to " + newServer);
                        }
                    }
                }
            }
        }
    }

    private void reassignKeysAfterServerRemoval(String server) {
        // Retrieve keys mapped to the removed server
        List<String> keysToReassign = serverToKeys.remove(server);
        if (keysToReassign == null || keysToReassign.isEmpty()) {
            System.out.println("No keys were assigned to " + server);
            return;
        }
        // Reassign each key to the new responsible server
        for (String key : keysToReassign) {
            String newServer = getServer(key); // Find the new server for the key
            serverToKeys.get(newServer).add(key); // Assign key to the new server
            System.out.println("Key " + key + " reassigned from " + server + " to " + newServer);
        }
    }

    // Display all keys stored on each server
    public void displayServerKeys() {
        for (Map.Entry<String, List<String>> entry : serverToKeys.entrySet()) {
            System.out.println("Server: " + entry.getKey() + ", Keys: " + entry.getValue());
        }
    }

    public static void main(String[] args) {
        // Initialize consistent hashing with 3 virtual nodes per server
        ConsistentHashing ch = new ConsistentHashing(3);

        // Add servers
        ch.addServer("Server1");
        ch.addServer("Server2");
        ch.addServer("Server3");

        // Assign keys to servers
        String[] keys = {"Key1", "Key2", "Key3", "Key4", "Key5"};
        for (String key : keys) {
            ch.assignKey(key);
        }

        // Display keys stored in each server
        System.out.println("\nInitial Key Distribution:");
        ch.displayServerKeys();

        // Add a new server
        System.out.println("\nAdding Server4...\n");
        ch.addServer("Server4");

        // Display updated keys stored in each server
        System.out.println("\nUpdated Key Distribution:");
        ch.displayServerKeys();

        // Remove a server
        System.out.println("\nRemoving Server2...\n");
        ch.removeServer("Server2");

        // Display final keys stored in each server
        System.out.println("\nFinal Key Distribution:");
        ch.displayServerKeys();
    }
}
