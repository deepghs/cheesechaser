import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue, Full, Empty
from threading import Thread, Event
from typing import Optional, Union, Any, Iterator

from tqdm import tqdm

from ..datapool import DataPool, ResourceNotFoundError, InvalidResourceDataError


@dataclass
class PipeItem:
    id: Union[int, str]
    data: Any
    order_id: int
    metainfo: Optional[dict]


@dataclass
class PipeError:
    id: Union[int, str]
    error: Exception
    order_id: int
    metainfo: Optional[dict]


class PipeSession:
    def __init__(self, queue: Queue, is_start: Event, is_stopped: Event, is_finished: Event):
        self.queue = queue
        self.is_start = is_start
        self.is_stopped = is_stopped
        self.is_finished = is_finished

    def next(self, block: bool = True, timeout: Optional[float] = None) -> PipeItem:
        if not self.is_start.is_set():
            self.is_start.set()
        return self.queue.get(block=block, timeout=timeout)

    def __iter__(self) -> Iterator[PipeItem]:
        while not (self.is_stopped.is_set() and self.queue.empty()):
            try:
                data = self.next(block=True, timeout=1.0)
                if isinstance(data, PipeItem):
                    yield data
            except Empty:
                pass

    def shutdown(self, wait=True, timeout: Optional[float] = None):
        self.is_stopped.set()
        if wait:
            self.is_finished.wait(timeout=timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown(wait=True, timeout=None)


class Pipe:
    def __init__(self, pool: DataPool):
        self.pool = pool

    def retrieve(self, resource_id, resource_metainfo):
        raise NotImplementedError  # pragma: no cover

    def batch_retrieve(self, resource_ids, max_workers: int = 12) -> PipeSession:
        pg = tqdm(resource_ids, desc='Batch Retrieving')
        queue = Queue(maxsize=max_workers * 3)
        is_started = Event()
        is_stopped = Event()
        is_finished = Event()

        def _func(order_id, resource_id, resource_metainfo):
            if is_stopped.is_set():
                return

            data, error = None, None
            try:
                try:
                    data = self.retrieve(resource_id, resource_metainfo)
                except ResourceNotFoundError as err:
                    logging.warning(f'Resource {resource_id!r} not found.')
                    error = err
                except InvalidResourceDataError as err:
                    logging.warning(f'Resource {resource_id!r} is invalid - {err}.')
                    error = err
                finally:
                    pg.update()
            except Exception as err:
                logging.error(f'Error occurred when retrieving resource {resource_id!r} - {err!r}')
                error = err

            try:
                if error is None:
                    item = PipeItem(
                        order_id=order_id,
                        id=resource_id,
                        data=data,
                        metainfo=resource_metainfo,
                    )
                else:
                    item = PipeError(
                        order_id=order_id,
                        id=resource_id,
                        error=error,
                        metainfo=resource_metainfo,
                    )

                while True:
                    try:
                        queue.put(item, block=True, timeout=1.0)
                    except Full:
                        if is_stopped.is_set():
                            break
                        continue
                    else:
                        break
            except Exception as err:
                logging.error(f'Error occurred when queuing resource {resource_id!r} - {err!r}')
                return

        def _productor():
            while True:
                if not is_started.wait(timeout=1.0):
                    if is_stopped.is_set():
                        return
                    else:
                        continue
                else:
                    break
            tp = ThreadPoolExecutor(max_workers=max_workers)
            for oid, ritem in enumerate(resource_ids):
                if is_stopped.is_set():
                    break
                if isinstance(ritem, tuple):
                    rid, rinfo = ritem
                else:
                    rid, rinfo = ritem, None
                tp.submit(_func, oid, rid, rinfo)

            tp.shutdown(wait=True)
            is_stopped.set()
            is_finished.set()

        t_productor = Thread(target=_productor)
        t_productor.start()

        return PipeSession(
            queue=queue,
            is_start=is_started,
            is_stopped=is_stopped,
            is_finished=is_finished
        )
