import os
import ftplib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import threading
import tempfile
import math
import time

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
        self.num_chunks = 5  # Fixed number of chunks
        self.bandwidth_bytes_per_sec = 100 * 1024 * 1024 // 8  # 100 Mb/s = 12.5 MB/s

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
        """Send a file to another node's disk using FTP with 5 chunks and 100 Mb/s bandwidth limit."""
        if target_ip not in self.ip_map:
            return f"Error: Target IP {target_ip} not found"
        if source_ip == target_ip:
            return f"Error: Cannot send file to self"
        if filename not in virtual_disk:
            return f"Error: File {filename} does not exist"

        size = virtual_disk[filename]
        can_store, error = self.check_target_storage(target_ip, size, 1024 * 1024 * 1024)  # 1 GB
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

            # Calculate chunk size (divide file into 5 chunks)
            chunk_size = math.ceil(size / self.num_chunks)  # Round up to ensure all bytes are sent
            sent_bytes = 0
            chunk_count = 0
            with open(temp_file_path, 'rb') as f:
                while chunk_count < self.num_chunks and sent_bytes < size:
                    chunk_count += 1
                    remaining_bytes = size - sent_bytes
                    current_chunk_size = min(chunk_size, remaining_bytes)  # Last chunk may be smaller
                    chunk = f.read(current_chunk_size)
                    if not chunk:
                        break
                    # Write chunk to a temporary file
                    with tempfile.NamedTemporaryFile(delete=False) as chunk_file:
                        chunk_file.write(chunk)
                        chunk_file_path = chunk_file.name
                    # Send chunk
                    start_time = time.time()
                    with open(chunk_file_path, 'rb') as cf:
                        mode = 'STOR' if chunk_count == 1 else 'APPE'
                        ftp.storbinary(f"{mode} {filename}", cf)
                    os.unlink(chunk_file_path)
                    sent_bytes += current_chunk_size
                    # Calculate time taken and enforce bandwidth limit
                    elapsed_time = time.time() - start_time
                    expected_time = current_chunk_size / self.bandwidth_bytes_per_sec
                    sleep_time = max(0, expected_time - elapsed_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    total_time = time.time() - start_time
                    print(f"Sent chunk {chunk_count}/5 ({current_chunk_size} bytes) for {filename} to {target_ip} in {total_time:.2f} seconds")

            ftp.quit()
            os.unlink(temp_file_path)
            print(f"Completed sending {filename} ({size} bytes) in {chunk_count} chunks to {target_ip}")
            return f"Sent {filename} ({size} bytes) to {target_ip}"
        except Exception as e:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return f"Error sending file to {target_ip}: {e}"