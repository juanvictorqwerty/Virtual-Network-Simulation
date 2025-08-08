import os
import ftplib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import threading
import tempfile
import math
import time
import re

class CustomFTPHandler(FTPHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_filename = None
        self.expected_chunks = 5
        self.received_chunks = 0
        self.temp_file_path = None

    def on_file_received(self, file_path):
        """Called when a file or chunk is received via STOR or APPE command."""
        if file_path.endswith("disk_metadata.json"):
            return  # Skip metadata file

        # Read the received chunk
        with open(file_path, 'rb') as f:
            data = f.read()
        
        # Parse the header (format: CHUNK:<number>:<size>\n)
        header_pattern = re.compile(b"CHUNK:(\d+):(\d+)\n")
        match = header_pattern.match(data)
        if not match:
            print(f"Error: Invalid chunk header in {file_path}")
            return

        chunk_number = int(match.group(1))
        chunk_size = int(match.group(2))
        payload = data[match.end():]  # Data after the header
        actual_payload_size = len(payload)

        if actual_payload_size != chunk_size:
            print(f"Error: Chunk {chunk_number} size mismatch, expected {chunk_size}, got {actual_payload_size}")
            return

        filename = os.path.basename(file_path)
        if chunk_number == 1:
            # Initialize for new file
            self.current_filename = filename
            self.received_chunks = 1
            self.temp_file_path = os.path.join(os.path.dirname(file_path), f"temp_{filename}")
            with open(self.temp_file_path, 'wb') as f:
                f.write(payload)
        else:
            # Validate chunk number and filename
            if filename != self.current_filename:
                print(f"Error: Chunk {chunk_number} for {filename} does not match expected file {self.current_filename}")
                return
            if chunk_number != self.received_chunks + 1:
                print(f"Error: Received chunk {chunk_number} out of order, expected {self.received_chunks + 1}")
                return
            self.received_chunks += 1
            # Append payload to temporary file
            with open(self.temp_file_path, 'ab') as f:
                f.write(payload)

        # If all chunks are received, finalize the file
        if self.received_chunks == self.expected_chunks:
            final_path = os.path.join(os.path.dirname(file_path), filename)
            os.rename(self.temp_file_path, final_path)
            size = os.path.getsize(final_path)
            self.server.node.virtual_disk[filename] = size
            self.server.node._save_disk()
            print(f"Updated virtual_disk with {filename}: {size} bytes")
            self.current_filename = None
            self.received_chunks = 0
            self.temp_file_path = None

        # Remove the temporary chunk file
        os.remove(file_path)

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
        self.header_size = 16  # Fixed header size: "CHUNK:<num>:<size>\n"

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
        """Send a file to another node's disk using FTP with 5 chunks, encapsulation, and 100 Mb/s bandwidth limit."""
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

            # Record start time
            start_time = time.time()
            print(f"Transfer started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

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
                    # Create header (format: CHUNK:<number>:<size>\n, padded to 16 bytes)
                    header = f"CHUNK:{chunk_count}:{current_chunk_size}\n".encode()
                    header = header.ljust(self.header_size, b'\0')
                    # Combine header and payload
                    chunk_with_header = header + chunk
                    # Write chunk to a temporary file
                    with tempfile.NamedTemporaryFile(delete=False) as chunk_file:
                        chunk_file.write(chunk_with_header)
                        chunk_file_path = chunk_file.name
                    # Send chunk
                    chunk_start_time = time.time()
                    with open(chunk_file_path, 'rb') as cf:
                        mode = 'STOR' if chunk_count == 1 else 'APPE'
                        ftp.storbinary(f"{mode} {filename}", cf)
                    os.unlink(chunk_file_path)
                    sent_bytes += current_chunk_size
                    # Enforce bandwidth limit
                    elapsed_time = time.time() - chunk_start_time
                    expected_time = current_chunk_size / self.bandwidth_bytes_per_sec
                    sleep_time = max(0, expected_time - elapsed_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    total_time = time.time() - chunk_start_time
                    print(f"Sent chunk {chunk_count}/5 ({current_chunk_size} bytes) for {filename} to {target_ip} in {total_time:.2f} seconds")

            # Record end time and print transfer details
            end_time = time.time()
            ftp.quit()
            os.unlink(temp_file_path)
            print(f"Transfer ended at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
            print(f"Transferred {filename}: {size} bytes ({size / (1024 * 1024):.2f} MB)")
            print(f"Completed sending {filename} ({size} bytes) in {chunk_count} chunks to {target_ip}")
            return f"Sent {filename} ({size} bytes) to {target_ip}"
        except Exception as e:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return f"Error sending file to {target_ip}: {e}"