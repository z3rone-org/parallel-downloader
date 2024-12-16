from concurrent.futures import ThreadPoolExecutor
import fsspec
import os
from time import sleep
from urllib.parse import urlparse
import logging


def download_file(uri, dst_dir, buffer_size=1024*1024):
    with fsspec.open(uri, mode="rb") as remote_file:
        filename = os.path.basename(uri)
        dst_path = os.path.join(dst_dir, filename)
        with open(dst_path, mode="wb") as local_file:
            while chunk := remote_file.read(buffer_size):
                local_file.write(chunk)

def download(uri, dst_dir, filter=None, parallel=3, retry=True, buffer_size=1024*1024, logger=None):
    if logger is None:
        logger = logging.getLogger("dummy_logger")
        logger.addHandler(logging.NullHandler())
    uri_parts = urlparse(uri)
    server = f'{uri_parts.scheme}://{uri_parts.netloc}'
    files = fsspec.open_files(uri).fs.ls(uri)
    with ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = dict()
        for f in files:
            file_path = urlparse(f['name']).path
            remote_path = f"{server}/{file_path}"
            if type(filter) is str:
                if filter not in remote_path:
                    continue
            if f['type'] == 'file':
                futures[remote_path] = executor.submit(
                    download_file,
                    uri=remote_path,
                    dst_dir=dst_dir,
                    buffer_size=buffer_size
                )

        while len(futures) > 0:
            for remote_path, future in futures.items():
                if future.done():
                    error=future.exception()
                    if error:
                        if retry:
                            futures[remote_path] = executor.submit(
                                download_file,
                                uri=remote_path,
                                dst_dir=dst_dir,
                                buffer_size=buffer_size
                            )
                            logger.warning(f'RETRY {remote_path}')
                        else:
                            logger.error(f'FAILED {remote_path}')
                        logger.error(error)
                    else:
                        del futures[remote_path]
                        logger.info(f'DONE {remote_path}')
            sleep(1)