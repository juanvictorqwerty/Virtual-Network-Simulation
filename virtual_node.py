import os
import json
from virtual_network import VirtualNetwork

class VirtualNode:
    def __init__(self, name, disk_path, ip_address, ftp_port):
        self.name = name
        self.disk_path = disk_path
        self.ip_address = ip_address
        self.ftp_port = ftp_port
        self.total_storage = 1024 * 1024 * 1024  # 100 MB in bytes
        self.virtual_disk = {}  # Dictionary to simulate disk (filename: size)
        self.memory = {}  # Dictionary to simulate RAM (variable: value)
        self.is_running = False  # VM running state
        self.ip_map = {
            "192.168.1.1": {"disk_path": "./assets/node1/", "ftp_port": 2121},
            "192.168.1.2": {"disk_path": "./assets/node2/", "ftp_port": 2122},
            "192.168.1.3": {"disk_path": "./assets/node3/", "ftp_port": 2123}
        }  # Mapping of IP addresses to disk paths and FTP ports
        self.network = VirtualNetwork()
        self._initialize_disk()
        self.network.start_ftp_server(self, ip_address, ftp_port, disk_path)
        self.start()  # Automatically start the VM on initialization

    def _initialize_disk(self):
        """Initialize or load the virtual disk from disk_path."""
        os.makedirs(self.disk_path, exist_ok=True)
        metadata_path = os.path.join(self.disk_path, "disk_metadata.json")
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    self.virtual_disk = json.load(f)
                    # Ensure sizes are integers
                    self.virtual_disk = {k: int(v) for k, v in self.virtual_disk.items()}
            except (json.JSONDecodeError, IOError):
                print(f"Warning: Could not load metadata from {metadata_path}. Starting with empty disk.")
                self.virtual_disk = {}
        else:
            self.virtual_disk = {}
            self._save_disk()
        # Sync virtual_disk with actual files
        for filename in os.listdir(self.disk_path):
            if filename != "disk_metadata.json":
                file_path = os.path.join(self.disk_path, filename)
                if os.path.isfile(file_path):
                    size = os.path.getsize(file_path)
                    self.virtual_disk[filename] = size
        self._save_disk()

    def _save_disk(self):
        """Save the virtual disk metadata to disk_path."""
        metadata_path = os.path.join(self.disk_path, "disk_metadata.json")
        try:
            with open(metadata_path, 'w') as f:
                json.dump(self.virtual_disk, f)
        except IOError as e:
            print(f"Error saving metadata to {metadata_path}: {e}")

    def _check_storage(self, size):
        """Check if there's enough storage for a given size."""
        used_storage = sum(self.virtual_disk.values())
        return used_storage + size <= self.total_storage

    def send(self, filename, target_ip):
        """Send a file to another node's disk using the VirtualNetwork."""
        if not self.is_running:
            return f"Error: VM {self.name} is not running"
        return self.network.send_file(filename, self.ip_address, target_ip, self.virtual_disk)

    def start(self):
        """Start the virtual machine."""
        if self.is_running:
            return f"VM {self.name} is already running"
        self.is_running = True
        return f"VM {self.name} started"

    def stop(self):
        """Stop the virtual machine."""
        if not self.is_running:
            return f"VM {self.name} is already stopped"
        self.is_running = False
        self._save_disk()
        self.network.stop_ftp_server(self.ip_address)
        return f"VM {self.name} stopped"

    def ls(self):
        """List all files in the virtual disk."""
        if not self.is_running:
            return f"Error: VM {self.name} is not running"
        if not self.virtual_disk:
            return "Directory is empty"
        return "\n".join(f"{name}: {size} bytes" for name, size in self.virtual_disk.items())

    def touch(self, filename, size=0):
        """Create a new file with optional size or update timestamp if it exists."""
        if not self.is_running:
            return f"Error: VM {self.name} is not running"
        try:
            size = int(size)  # Ensure size is an integer
            if size < 0:
                return "Error: Size cannot be negative"
        except ValueError:
            return "Error: Size must be an integer"
        if filename not in self.virtual_disk:
            if not self._check_storage(size * 1024 * 1024):
                return f"Error: Not enough storage on disk"
            file_path = os.path.join(self.disk_path, filename)
            with open(file_path, 'wb') as f:
                f.write(b"\0" * size)
            self.virtual_disk[filename] = size
            self._save_disk()
            return f"Created file: {filename} with {size} bytes"
        else:
            os.utime(os.path.join(self.disk_path, filename))
            return f"Updated timestamp for file: {filename}"

    def trunc(self, filename, size=0):
        """Truncate file to specified size or 0 if no size provided."""
        if not self.is_running:
            return f"Error: VM {self.name} is not running"
        try:
            size = int(size)  # Ensure size is an integer
            if size < 0:
                return "Error: Size cannot be negative"
        except ValueError:
            return "Error: Size must be an integer"
        if filename in self.virtual_disk:
            if not self._check_storage(size - self.virtual_disk[filename]):
                return f"Error: Not enough storage on disk"
            file_path = os.path.join(self.disk_path, filename)
            with open(file_path, 'wb') as f:
                f.write(b"\0" * size)
            self.virtual_disk[filename] = size
            self._save_disk()
            return f"Truncated {filename} to {size} bytes"
        else:
            return f"File {filename} does not exist"

    def set_var(self, var_name, value):
        """Set a variable in memory."""
        if not self.is_running:
            return f"Error: VM {self.name} is not running"
        try:
            self.memory[var_name] = int(value)  # Store as integer for simplicity
            return f"Set {var_name} = {value} in memory as an integer"
        except ValueError:
            return f"Error: Value must be an integer"

    def get_var(self, var_name):
        """Get a variable from memory."""
        if not self.is_running:
            return f"Error: VM {self.name} is not running"
        if var_name in self.memory:
            return f"{var_name} = {self.memory[var_name]}"
        else:
            return f"Variable {var_name} not found in memory"

    def execute_instruction(self, instruction):
        """Simulate CPU by executing a single instruction."""
        if not self.is_running:
            return f"Error: VM {self.name} is not running"
        parts = instruction.strip().split()
        if not parts:
            return "No instruction provided"
        cmd = parts[0].lower()
        if cmd == "add":
            if len(parts) == 3:
                var1, var2 = parts[1], parts[2]
                if var1 in self.memory and var2 in self.memory:
                    result = self.memory[var1] + self.memory[var2]
                    self.memory["result"] = result
                    return f"Added {var1} + {var2}, stored result = {result}"
                return "Error: Variables not found"
            return "Error: Usage: add <var1> <var2>"
        return "Unknown instruction"

    def __str__(self):
        status = "running" if self.is_running else "stopped"
        return f"VirtualNode({self.name}, IP: {self.ip_address}, Status: {status}, Files: {len(self.virtual_disk)}, Memory: {len(self.memory)} variables)"

    def run_interactive(self):
        """Run an interactive loop to process VM commands."""
        print(self)  # Print initial VM status
        while self.is_running:
            try:
                command = input(">>> ").strip().split()
                if not command:
                    continue
                cmd = command[0].lower()
                if cmd == "ls":
                    print(self.ls())
                elif cmd == "touch" and len(command) > 1:
                    size = int(command[2]) if len(command) > 2 and command[2].isdigit() else 0
                    print(self.touch(command[1], size))
                elif cmd == "trunc" and len(command) > 1:
                    size = int(command[2]) if len(command) > 2 and command[2].isdigit() else 0
                    print(self.trunc(command[1], size))
                elif cmd == "send" and len(command) == 3:
                    print(self.send(command[1], command[2]))
                elif cmd == "set" and len(command) == 3:
                    print(self.set_var(command[1], command[2]))
                elif cmd == "get" and len(command) == 2:
                    print(self.get_var(command[1]))
                elif cmd == "add" and len(command) == 3:
                    print(self.execute_instruction(" ".join(command)))
                elif cmd == "stop":
                    print(self.stop())
                    break
                else:
                    print("Invalid command. Use: ls, touch <filename> [size], trunc <filename> [size], send <filename> <ip_address>, set <var> <value>, get <var>, add <var1> <var2>, stop")
            except EOFError:
                print("\nEOF detected. Stopping VM.")
                print(self.stop())
                break
            except KeyboardInterrupt:
                print("\nKeyboard interrupt detected. Stopping VM.")
                print(self.stop())
                break
            except Exception as e:
                print(f"Error processing command: {e}")