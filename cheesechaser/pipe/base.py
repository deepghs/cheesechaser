"""
This module provides a data pipeline system for retrieving and processing resources from a data pool.

It includes classes for managing pipeline items, errors, sessions, and the main pipeline itself.
The pipeline allows for concurrent retrieval of resources using a thread pool and provides
a convenient interface for iterating over retrieved items.

Key components:

- ``PipeItem``: Represents a successfully retrieved resource.
- ``PipeError``: Represents an error that occurred during resource retrieval.
- ``PipeSession``: Manages the pipeline session, including item iteration and shutdown.
- ``Pipe``: The main pipeline class for retrieving resources from a data pool.

This module is designed to work with large datasets and provides error handling and
progress tracking capabilities.
"""

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
    """
    Represents a successfully retrieved resource item in the pipeline.

    :param id: The unique identifier of the resource.
    :type id: Union[int, str]
    :param data: The actual data of the resource.
    :type data: Any
    :param order_id: The order ID of the resource in the retrieval sequence.
    :type order_id: int
    :param metainfo: Additional metadata associated with the resource.
    :type metainfo: Optional[dict]
    """
    id: Union[int, str]
    data: Any
    order_id: int
    metainfo: Optional[dict]


@dataclass
class PipeError:
    """
    Represents an error that occurred during resource retrieval in the pipeline.

    :param id: The unique identifier of the resource that caused the error.
    :type id: Union[int, str]
    :param error: The exception that was raised during retrieval.
    :type error: Exception
    :param order_id: The order ID of the resource in the retrieval sequence.
    :type order_id: int
    :param metainfo: Additional metadata associated with the resource.
    :type metainfo: Optional[dict]
    """
    id: Union[int, str]
    error: Exception
    order_id: int
    metainfo: Optional[dict]


class PipeSession:
    """
    Manages a pipeline session, providing methods to iterate over retrieved items and control the session.

    :param queue: The queue containing retrieved items.
    :type queue: Queue
    :param is_start: An event indicating whether the session has started.
    :type is_start: Event
    :param is_stopped: An event indicating whether the session has been stopped.
    :type is_stopped: Event
    :param is_finished: An event indicating whether the session has finished.
    :type is_finished: Event
    :param max_count: Max item count for iterating from the data source. Unlimited when not given.
    :type max_count: Optional[int]
    """

    def __init__(self, queue: Queue, is_start: Event, is_stopped: Event, is_finished: Event,
                 max_count: Optional[int] = None):
        self.queue = queue
        self.is_start = is_start
        self.is_stopped = is_stopped
        self.is_finished = is_finished
        self.max_count: Optional[int] = max_count
        self._current_count: int = 0

    def next(self, block: bool = True, timeout: Optional[float] = None) -> PipeItem:
        """
        Retrieve the next item from the pipeline.

        :param block: Whether to block until an item is available.
        :type block: bool
        :param timeout: The maximum time to wait for an item.
        :type timeout: Optional[float]
        :return: The next PipeItem in the queue.
        :rtype: PipeItem
        :raises Empty: If no item is available within the specified timeout.
        """
        if not self.is_start.is_set():
            self.is_start.set()
        return self.queue.get(block=block, timeout=timeout)

    def _count_update(self, n: int = 1) -> bool:
        """
        Update current count. If the count reaches the limit, set the status to ``stopped``.

        :param n: Count for Adding. Default is 1.
        :return: Reached the limit or not
        :rtype: bool
        """
        self._current_count += n
        if self.max_count is not None and self._current_count >= self.max_count:
            self.is_stopped.set()
            return True
        else:
            return False

    def __iter__(self) -> Iterator[PipeItem]:
        """
        Iterate over the items in the pipeline.

        :return: An iterator of PipeItems.
        :rtype: Iterator[PipeItem]
        """
        pg = tqdm(desc='Piped Items', total=self.max_count)
        if self._count_update(0):
            return

        while not (self.is_stopped.is_set() and self.queue.empty()):
            try:
                data = self.next(block=True, timeout=1.0)
                if isinstance(data, PipeItem):
                    pg.update()
                    yield data
                    if self._count_update():
                        break
            except Empty:
                pass

    def shutdown(self, wait=True, timeout: Optional[float] = None):
        """
        Shutdown the pipeline session.

        :param wait: Whether to wait for the session to finish.
        :type wait: bool
        :param timeout: The maximum time to wait for the session to finish.
        :type timeout: Optional[float]
        """
        self.is_stopped.set()
        if wait:
            self.is_finished.wait(timeout=timeout)

    def __enter__(self):
        """
        Enter the context manager.

        :return: The PipeSession instance.
        :rtype: PipeSession
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager, shutting down the session.

        :param exc_type: The type of the exception that caused the context to be exited.
        :param exc_val: The instance of the exception that caused the context to be exited.
        :param exc_tb: A traceback object encoding the stack trace.
        """
        self.shutdown(wait=True, timeout=None)


class Pipe:
    """
    The main pipeline class for retrieving resources from a data pool.

    :param pool: The data pool to retrieve resources from.
    :type pool: DataPool
    """

    def __init__(self, pool: DataPool):
        self.pool = pool

    def retrieve(self, resource_id, resource_metainfo, silent: bool = False):
        """
        Retrieve a single resource from the data pool.

        This method should be implemented by subclasses.

        :param resource_id: The ID of the resource to retrieve.
        :param resource_metainfo: Additional metadata for the resource.
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :raises NotImplementedError: If not implemented by a subclass.
        """
        raise NotImplementedError  # pragma: no cover

    def batch_retrieve(self, resource_ids, max_workers: int = 12, max_count: Optional[int] = None,
                       silent: bool = False) -> PipeSession:
        """
        Retrieve multiple resources in parallel using a thread pool.

        :param resource_ids: An iterable of resource IDs or (ID, metainfo) tuples to retrieve.
        :param max_workers: The maximum number of worker threads to use.
        :type max_workers: int
        :param max_count: Max item count for iterating from the data source. Unlimited when not given.
        :type max_count: Optional[int]
        :param silent: If True, suppresses progress bar of each standalone files during the mocking process.
        :type silent: bool
        :return: A PipeSession object for iterating over the retrieved items.
        :rtype: PipeSession
        """
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
                    data = self.retrieve(resource_id, resource_metainfo, silent=silent)
                except ResourceNotFoundError as err:
                    logging.warning(f'Resource {resource_id!r} not found.')
                    error = err
                except InvalidResourceDataError as err:
                    logging.warning(f'Resource {resource_id!r} is invalid - {err}.')
                    error = err
                finally:
                    pg.update()
            except Exception as err:
                logging.exception(f'Error occurred when retrieving resource {resource_id!r} - {err!r}')
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
                logging.exception(f'Error occurred when queuing resource {resource_id!r} - {err!r}')
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
            is_finished=is_finished,
            max_count=max_count,
        )
