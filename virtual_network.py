import os
import ftplib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import threading
import tempfile

class CustomFTPHandler(FTPHandler):
    def on_file_received(self, file_path):
        """Called when a file is received via STOR command."""
        filename = os.path.basename(file_path)
        if filename != "disk_metadata.json":  # Exclude metadata file
            size = os.path.getsize(file_path)
            # The VirtualNode instance is attached to the server instance.
            self.server.node.virtual_disk[filename] = size
            self.server.node._save_disk()
            print(f"Updated virtual_disk with {filename}: {size} bytes")

class VirtualNetwork:
    def __init__(self):
        self.ip_map = {
            "192.168.1.1": {"disk_path": "./assets/node1/", "ftp_port": 2121},
            "192.168.1.2": {"disk_path": "./assets/node2/", "ftp_port": 2122},
            "192.168.1.3": {"disk_path": "./assets/node3/", "ftp_port": 2123}
        }
        self.ftp_servers = {}

    def start_ftp_server(self, node, ip_address, ftp_port, disk_path):
        """Start an FTP server for a given node."""
        authorizer = DummyAuthorizer()
        authorizer.add_user("user", "password", disk_path, perm="elradfmw")
        handler = CustomFTPHandler
        handler.authorizer = authorizer
        ftp_server = FTPServer(("0.0.0.0", ftp_port), handler)
        ftp_server.node = node  # Attach the VirtualNode instance to the server
        self.ftp_servers[ip_address] = ftp_server
        ftp_thread = threading.Thread(target=ftp_server.serve_forever, daemon=True)
        ftp_thread.start()
        print(f"FTP server started on {ip_address}:{ftp_port}")

    def stop_ftp_server(self, ip_address):
        """Stop the FTP server for a given IP address."""
        if ip_address in self.ftp_servers:
            self.ftp_servers[ip_address].close_all()
            print(f"FTP server stopped for {ip_address}")
            del self.ftp_servers[ip_address]

    def check_target_storage(self, target_ip, size, total_storage):
        """Check if the target node has enough storage via FTP."""
        if target_ip not in self.ip_map:
            return False, f"Error: Target IP {target_ip} not found"
        try:
            ftp = ftplib.FTP()
            ftp.connect(host="127.0.0.1", port=self.ip_map[target_ip]["ftp_port"])
            ftp.login(user="user", passwd="password")
            # List files to calculate used storage
            files = []
            ftp.dir(lambda x: files.append(x))
            used_storage = 0
            for line in files:
                parts = line.split()
                if len(parts) > 4 and parts[0].startswith("-"):
                    file_size = int(parts[4])
                    if parts[-1] != "disk_metadata.json":
                        used_storage += file_size
            ftp.quit()
            if used_storage + size <= total_storage:
                return True, None
            return False, f"Error: Not enough storage on {target_ip}'s disk"
        except Exception as e:
            return False, f"Error checking storage on {target_ip}: {e}"

    def send_file(self, filename, source_ip, target_ip, virtual_disk):
        """Send a file to another node's disk using FTP."""
        if target_ip not in self.ip_map:
            return f"Error: Target IP {target_ip} not found"
        if source_ip == target_ip:
            return f"Error: Cannot send file to self"
        if filename not in virtual_disk:
            return f"Error: File {filename} does not exist"

        size = virtual_disk[filename]
        can_store, error = self.check_target_storage(target_ip, size, 100 * 1024 * 1024)  # 100 MB
        if not can_store:
            return error

        # Create a temporary file with the content
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"\0" * size)
            temp_file_path = temp_file.name

        try:
            # Connect to target FTP server
            ftp = ftplib.FTP()
            ftp.connect(host="127.0.0.1", port=self.ip_map[target_ip]["ftp_port"])
            ftp.login(user="user", passwd="password")
            # Check if file already exists
            file_list = ftp.nlst()
            if filename in file_list:
                ftp.quit()
                os.unlink(temp_file_path)
                return f"Error: File {filename} already exists on {target_ip}"
            # Upload file
            with open(temp_file_path, 'rb') as f:
                ftp.storbinary(f"STOR {filename}", f)
            ftp.quit()
            os.unlink(temp_file_path)
            return f"Sent {filename} ({size} bytes) to {target_ip}"
        except Exception as e:
            os.unlink(temp_file_path)
            return f"Error sending file to {target_ip}: {e}"