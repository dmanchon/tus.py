
from __future__ import print_function
import os
import base64
import logging
import argparse

import aiohttp
import asyncio
import abc

LOG_LEVEL = logging.INFO
DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
TUS_VERSION = '1.0.0'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.NullHandler())

class TusStream(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def read(self, offset, size):
        raise NotImplementedError()

    @abc.abstractmethod
    async def size(self):
        raise NotImplementedError()

class FileStream(TusStream):
    def __init__(self, filename):
        self._file = open(filename, 'rb')
        self._file.seek(0, os.SEEK_END)
        self._size = self._file.tell()
        self._file.seek(0)
        
    async def read(self, offset, size):
        self._file.seek(offset)
        data = self._file.read(size)
        return data

    async def size(self):
        return self._size
        

class TusError(Exception):
    def __init__(self, message, response=None):
        super(TusError, self).__init__(message)
        self.response = response


def _init():
    fmt = "[%(asctime)s] %(levelname)s %(message)s"
    h = logging.StreamHandler()
    h.setLevel(LOG_LEVEL)
    h.setFormatter(logging.Formatter(fmt))
    logger.addHandler(h)


class DictAction(argparse.Action):
    def __init__(self, option_strings, dest, **kwargs):
        super(DictAction, self).__init__(
            option_strings, dest, nargs=2, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        key, value = values[0], values[1]
        d = getattr(namespace, self.dest)
        if d is None:
            setattr(namespace, self.dest, {})
        d = getattr(namespace, self.dest)
        d[key] = value


def _create_parent_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('file', type=argparse.FileType('rb'))
    parser.add_argument('--chunk-size', type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument(
        '--header',
        dest='headers',
        action=DictAction,
        help="A single key/value pair as 2 arguments"
        " to be sent as HTTP header."
        " Can be specified multiple times to send multiple headers.")
    return parser


async def async_cmd_upload():
    _init()

    parser = _create_parent_parser()
    parser.add_argument('tus_endpoint')
    parser.add_argument(
        '--file_name',
        help="Override uploaded file name."
        "Inferred from local file name if not specified.")
    parser.add_argument(
        '--metadata',
        action=DictAction,
        help="A single key/value pair as 2 arguments"
        " to be sent in Upload-Metadata header."
        " Can be specified multiple times to send more than one pair.")
    args = parser.parse_args()

    file_name = args.file_name or os.path.basename(args.file.name)
    file_size = _get_file_size(args.file)

    file_endpoint = await create(
        args.tus_endpoint,
        file_name,
        file_size,
        headers=args.headers,
        metadata=args.metadata)

    print(file_endpoint)

    obj = FileStream(args.file.name)

    await resume(
        obj,
        file_endpoint,
        chunk_size=args.chunk_size,
        headers=args.headers,
        offset=0)

def _cmd_upload():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_cmd_upload())

async def async_cmd_resume():
    _init()

    parser = _create_parent_parser()
    parser.add_argument('file_endpoint')
    args = parser.parse_args()

    obj = FileStream(args.file.name)

    await resume(
        obj,
        args.file_endpoint,
        chunk_size=args.chunk_size,
        headers=args.headers)

def _cmd_resume():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_cmd_resume())


async def upload(obj,
           tus_endpoint,
           chunk_size=DEFAULT_CHUNK_SIZE,
           file_name=None,
           headers=None,
           metadata=None):

    file_name = file_name or os.path.basename(file_obj.name)
    file_size = _get_file_size(file_obj)

    file_endpoint = await create(
        tus_endpoint,
        file_name,
        file_size,
        headers=headers,
        metadata=metadata)

    await resume(
        obj,
        file_endpoint,
        chunk_size=chunk_size,
        headers=headers,
        offset=0)


def _get_file_size(f):
    pos = f.tell()
    f.seek(0, os.SEEK_END)
    size = f.tell()
    f.seek(pos)
    return size


async def create(tus_endpoint, file_name, file_size, headers=None, metadata=None):
    logger.info("Creating file endpoint")

    h = {
        "Tus-Resumable": TUS_VERSION,
        "Upload-Length": str(file_size),
    }

    if headers:
        h.update(headers)

    if metadata:
        pairs = [
            k + ' ' + base64.b64encode(v.encode('utf-8')).decode()
            for k, v in metadata.items()
        ]
        h["Upload-Metadata"] = ','.join(pairs)

    async with aiohttp.ClientSession() as client:
        async with client.post(tus_endpoint, headers=h) as response:
            if response.status != 201:
                raise TusError("Create failed: Status=%s" % response.status,
                               response=response)

            location = response.headers["Location"]
            logger.info("Created: %s", location)
            return location
        
async def resume(stream_obj,
                  file_endpoint,
                  chunk_size=DEFAULT_CHUNK_SIZE,
                  headers=None,
                  offset=None):
    if offset is None:
        offset = await _get_offset(file_endpoint, headers=headers)

    total_sent = 0
    file_size = await stream_obj.size()
    while offset < file_size:
        data = await stream_obj.read(offset, chunk_size)
        offset = await _upload_chunk(data, offset, file_endpoint, headers=headers)
        total_sent += len(data)
        logger.info("Total bytes send: %i", total_sent)


async def _get_offset(file_endpoint, headers=None):
    logger.info("Getting offset")

    h = {"Tus-Resumable": TUS_VERSION}

    if headers:
        h.update(headers)

    async with aiohttp.ClientSession() as client:
        async with client.request('HEAD', file_endpoint, headers=h) as response:
            if response.status > 400:
                raise Exception()
      
            offset = int(response.headers["Upload-Offset"])
            logger.info("offset=%i", offset)
            return offset


async def _upload_chunk(data, offset, file_endpoint, headers=None):
    logger.info("Uploading %d bytes chunk from offset: %i", len(data), offset)

    h = {
        'Content-Type': 'application/offset+octet-stream',
        'Upload-Offset': str(offset),
        'Tus-Resumable': TUS_VERSION,
    }

    if headers:
        h.update(headers)

    async with aiohttp.ClientSession() as client:
        async with client.patch(file_endpoint, headers=h, data=data) as response:
            if response.status != 204:
                raise TusError("Upload chunk failed: Status=%s" % response.status_code,
                               response=response)

            return int(response.headers["Upload-Offset"])
