#!/usr/bin/env python3
import os, json, threading, argparse, requests
from hashlib import sha1
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
from steam.utils.web import make_requests_session
from steam.client.cdn import ContentServer, CDNDepotManifest, CDNClient
from steam.core.crypto import symmetric_decrypt
from binascii import crc32
from zipfile import ZipFile
from io import BytesIO
import struct, lzma, zstandard

CONTENT_SERVER_LIST = [
    "http://st.dl.bscstorage.net",
    "http://steampipe.steamcontent.tnkjmec.com",
    "http://alibaba.cdn.steampipe.steamcontent.com",
]

def get_manifest_request_code(manifest_gid):
    url = f"http://gmrc.openst.top/manifest/{manifest_gid}"
    return requests.get(url).content.decode('utf-8')

class MyCDNClient(CDNClient):
    def __init__(self):
        self.web = make_requests_session()
        self.depot_keys = {}
        self.manifests = []

        if not self.servers:
            for url in CONTENT_SERVER_LIST:
                parsed_url = urlparse(url)
                server = ContentServer()
                server.https = parsed_url.scheme == "https"
                server.host = parsed_url.hostname
                server.port = 443 if server.https else 80
                self.servers.append(server)

    def get_depot_key(self, app_id, depot_id):
        if depot_id not in self.depot_keys:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            depot_keys_file = os.path.join(current_dir, 'depotkeys.json')

            with open(depot_keys_file, 'r') as f:
                depot_keys = json.load(f)

            depot_key = depot_keys[str(depot_id)]
            self.depot_keys[depot_id] = bytes.fromhex(depot_key)
        return self.depot_keys[depot_id]

    def get_manifest(self, app_id, depot_id, manifest_gid):
        manifest_request_code = get_manifest_request_code(manifest_gid)
        resp = self.cdn_cmd('depot', '%s/manifest/%s/5/%s' % (depot_id, manifest_gid, manifest_request_code))
        manifest = self.DepotManifestClass(self, 0, resp.content)
        self.manifests.append(manifest)

    def get_manifest_for_workshop_item(self, item_id):
        data = {'itemcount': 1, 'publishedfileids[0]': item_id}
        api_url = "https://api.steampowered.com/ISteamRemoteStorage/GetPublishedFileDetails/v1/"
        resp = requests.post(api_url, data=data)
        details = resp.json()['response']['publishedfiledetails'][0]
        app_id = depot_id = details['consumer_app_id']
        manifest_gid = details['hcontent_file']
        return self.get_manifest(app_id, depot_id, manifest_gid)

    def get_chunk(self, app_id, depot_id, chunk_id):
        if (depot_id, chunk_id) not in self._chunk_cache:
            resp = self.cdn_cmd('depot', '%s/chunk/%s' % (depot_id, chunk_id))
            data = symmetric_decrypt(resp.content, self.get_depot_key(app_id, depot_id))

            if data[:3] == b'VZa':
                vzfilter = lzma._decode_filter_properties(lzma.FILTER_LZMA1, data[7:12])
                vzdec = lzma.LZMADecompressor(lzma.FORMAT_RAW, filters=[vzfilter])
                checksum, decompressed_size = struct.unpack('<II', data[-10:-2])
                data = vzdec.decompress(data[12:-9])[:decompressed_size]
                if crc32(data) != checksum:
                    raise ValueError("VZ: CRC32 checksum doesn't match for decompressed data")
            elif data[:4] == b'VSZa':
                zstd_dec = zstandard.ZstdDecompressor()
                checksum, decompressed_size = struct.unpack('<II', data[-15:-7])
                data = zstd_dec.decompress(data[8:-15])[:decompressed_size]
                if crc32(data) != checksum:
                    raise ValueError("VSZ: CRC32 checksum doesn't match for decompressed data")
            else:
                with ZipFile(BytesIO(data)) as zf:
                    data = zf.read(zf.filelist[0])

            self._chunk_cache[(depot_id, chunk_id)] = data
        return self._chunk_cache[(depot_id, chunk_id)]

    def download_files(self, download_path, max_workers=8):
        errors = []
        errors_lock = threading.Lock()

        def save_depot_file(depot_file):
            try:
                local_path = os.path.join(download_path, depot_file.filename)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                if not os.path.exists(local_path):
                    with open(local_path, 'wb') as f:
                        for chunk in iter(lambda: depot_file.read(16384), b''):
                            f.write(chunk)
                    print(f"✅ 下载成功: {depot_file.filename}")
                else:
                    local_size = os.path.getsize(local_path)
                    if local_size < depot_file.size:
                        depot_file.seek(local_size)
                        with open(local_path, 'ab') as f:
                            for chunk in iter(lambda: depot_file.read(16384), b''):
                                f.write(chunk)
                        print(f"✅ 下载成功: {depot_file.filename}")
                    else:
                        checksum = sha1()
                        with open(local_path, 'rb') as fp:
                            for chunk in iter(lambda: fp.read(16384), b''):
                                checksum.update(chunk)
                        if checksum.digest() != depot_file.file_mapping.sha_content:
                            raise ValueError(f"校验和不匹配: {local_path}")

            except Exception as e:
                with errors_lock:
                    errors.append(depot_file.filename)
                print(f"❌ 下载失败: {depot_file.filename}, 错误: {e}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for manifest in self.manifests:
                if manifest.filenames_encrypted:
                    manifest.decrypt_filenames(self.get_depot_key(0, manifest.depot_id))

                for depot_file in manifest:
                    if depot_file.is_file:
                        executor.submit(save_depot_file, depot_file)

        if errors:
            print("以下文件下载失败：")
            for filename in errors:
                print(filename)
            print(f"总共失败 {len(errors)} 个文件。")
        else:
            print("全部文件下载成功！")

def main():
    parser = argparse.ArgumentParser(description="Steam depot downloader")
    subparsers = parser.add_subparsers(dest='command', required=True)

    # download
    parser_download = subparsers.add_parser('download')
    parser_download.add_argument('manifest_path', help='manifest文件路径')
    parser_download.add_argument('download_path', help='下载目录')

    # download_depot
    parser_download_depot = subparsers.add_parser('download_depot')
    parser_download_depot.add_argument('app_id')
    parser_download_depot.add_argument('depot_id')
    parser_download_depot.add_argument('manifest_gid')
    parser_download_depot.add_argument('download_path', help='下载目录')

    # download_workshop
    parser_download_workshop = subparsers.add_parser('download_workshop')
    parser_download_workshop.add_argument('item_id')
    parser_download_workshop.add_argument('download_path', help='下载目录')

    client = MyCDNClient()
    args = parser.parse_args()
    if args.command == 'download':
        if not os.path.exists(args.manifest_path):
            print("manifest文件不存在:", args.manifest_path)
            exit(1)
        with open(args.manifest_path, "rb") as f:
            manifest = CDNDepotManifest(client, 0, f.read())
        client.manifests.append(manifest)
    elif args.command == 'download_depot':
        client.get_manifest(args.app_id, args.depot_id, args.manifest_gid)
    elif args.command == 'download_workshop':
        client.get_manifest_for_workshop_item(args.item_id)

    os.makedirs(args.download_path, exist_ok=True)
    client.download_files(args.download_path)

if __name__ == '__main__':
    main()
