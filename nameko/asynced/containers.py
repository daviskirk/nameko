import asyncio
from functools import partial
import inspect
import warnings
import sys
from logging import getLogger

from nameko import config, serialization
from nameko.asynced.extensions import (
    ENTRYPOINT_EXTENSIONS_ATTR, is_dependency, iter_extensions
)
from nameko.exceptions import ContainerBeingKilled
from nameko.constants import (
    CALL_ID_STACK_CONTEXT_KEY, DEFAULT_MAX_WORKERS,
    DEFAULT_PARENT_CALLS_TRACKED, MAX_WORKERS_CONFIG_KEY,
    PARENT_CALLS_CONFIG_KEY
)
from nameko.containers import ServiceContainer as SyncServiceContainer
from nameko.containers import get_service_name, WorkerContext, new_call_id
from nameko.log_helpers import make_timing_logger
from nameko.utils import import_from_path

_log = getLogger(__name__)
_log_time = make_timing_logger(_log)

is_method = inspect.isfunction


def get_container_cls():
    class_path = config.get('SERVICE_CONTAINER_CLS')
    return import_from_path(class_path) or ServiceContainer


async def call_each(call, items, abort_on_error=False):
    failed = False
    tasks = [asyncio.create_task(call(item)) for item in items]
    try:
        results = await asyncio.gather(*tasks)
    except Exception as e:
        if abort_on_error:
            for task in tasks:
                task.cancel()
        raise
    return results


class SpawningProxy(object):
    def __init__(self, items, abort_on_error=False):
        """ Wraps an iterable set of items such that a call on the returned
        SpawningProxy instance will spawn a call in a
        :class:`~eventlet.greenthread.GreenThread` for each item.

        Returns when every spawned thread has completed.

        :param items: Iterable item set to process
        :param abort_on_error: If True, any exceptions raised on an individual
            item call will cause all peer item call threads to be killed, and
            for the exception to be propagated to the caller immediately.
        """
        self._items = items
        self.abort_on_error = abort_on_error

    def __getattr__(self, name):

        async def spawning_method(*args, **kwargs):
            items = self._items
            if items:
                async def call(item):
                    return await getattr(item, name)(*args, **kwargs)
                return await call_each(call, self._items, self.abort_on_error)

        return spawning_method


class SpawningSet(set):
    """ A set with an ``.all`` property that will spawn a method call on each
    item in the set into its own (parallel) greenthread.
    """
    @property
    def all(self):
        return SpawningProxy(self)


class ServiceContainer:

    event_cls = asyncio.Event

    def __init__(self, service_cls):
        self.service_cls = service_cls

        self.service_name = get_service_name(service_cls)
        self.shared_extensions = {}

        self.max_workers = (
            config.get(MAX_WORKERS_CONFIG_KEY) or DEFAULT_MAX_WORKERS)

        self.serializer, self.accept = serialization.setup()

        self.entrypoints = SpawningSet()
        self.dependencies = SpawningSet()
        self.subextensions = SpawningSet()

        for attr_name, dependency in inspect.getmembers(service_cls,
                                                        is_dependency):
            bound = dependency.bind(self.interface, attr_name)
            self.dependencies.add(bound)
            self.subextensions.update(iter_extensions(bound))

        for method_name, method in inspect.getmembers(service_cls, is_method):
            entrypoints = getattr(method, ENTRYPOINT_EXTENSIONS_ATTR, [])
            for entrypoint in entrypoints:
                bound = entrypoint.bind(self.interface, method_name)
                self.entrypoints.add(bound)
                self.subextensions.update(iter_extensions(bound))

        self.started = False
        self._worker_threads = {}
        self._managed_threads = {}
        self._being_killed = False
        self._died = self.event_cls()

    @property
    def config(self):
        warnings.warn("Use ``nameko.config`` instead.", DeprecationWarning)
        return config

    @property
    def extensions(self):
        return SpawningSet(
            self.entrypoints | self.dependencies | self.subextensions
        )

    @property
    def interface(self):
        """ An interface to this container for use by extensions.
        """
        return self

    async def start(self):
        """ Start a container by starting all of its extensions.
        """
        _log.debug('starting %s', self)
        self.started = True

        with _log_time('started %s', self):
            await self.extensions.all.setup()
            await self.extensions.all.start()

    async def stop(self):
        """ Stop the container gracefully.

        First all entrypoints are asked to ``stop()``.
        This ensures that no new worker threads are started.

        It is the extensions' responsibility to gracefully shut down when
        ``stop()`` is called on them and only return when they have stopped.

        After all entrypoints have stopped the container waits for any
        active workers to complete.

        After all active workers have stopped the container stops all
        dependency providers.

        At this point there should be no more managed threads. In case there
        are any managed threads, they are killed by the container.
        """
        if self._died.is_set():
            _log.debug('already stopped %s', self)
            return

        if self._being_killed:
            # this race condition can happen when a container is hosted by a
            # runner and yields during its kill method; if it's unlucky in
            # scheduling the runner will try to stop() it before self._died
            # has a result
            _log.debug('already being killed %s', self)
            try:
                self._died.is_set()
            except:
                pass  # don't re-raise if we died with an exception
            return

        _log.debug('stopping %s', self)

        with _log_time('stopped %s', self):

            # entrypoint have to be stopped before dependencies to ensure
            # that running workers can successfully complete
            await self.entrypoints.all.stop()

            # it should be safe now to stop any dependency as there is no
            # active worker which could be using it
            await self.dependencies.all.stop()

            # finally, stop remaining extensions
            await self.subextensions.all.stop()

            # any any managed threads they spawned
            await self._kill_managed_threads()

            self.started = False

            # if `kill` is called after `stop`, they race to send this
            if not self._died.is_set():
                self._died.set()

    async def kill(self, exc_info=None):
        """ Kill the container in a semi-graceful way.

        Entrypoints are killed, followed by any active worker threads.
        Next, dependencies are killed. Finally, any remaining managed threads
        are killed.

        If ``exc_info`` is provided, the exception will be raised by
        :meth:`~wait``.
        """
        if self._being_killed:
            # this happens if a managed thread exits with an exception
            # while the container is being killed or if multiple errors
            # happen simultaneously
            _log.debug('already killing %s ... waiting for death', self)
            try:
                await self._died.wait()
            except:
                pass  # don't re-raise if we died with an exception
            return

        self._being_killed = True

        if self._died.is_set():
            _log.debug('already stopped %s', self)
            return

        if exc_info is not None:
            _log.info('killing %s due to %s', self, exc_info[1])
        else:
            _log.info('killing %s', self)

        # protect against extensions that throw during kill; the container
        # is already dying with an exception, so ignore anything else
        async def safely_kill_extensions(ext_set):
            try:
                await ext_set.kill()
            except Exception as exc:
                _log.warning('Extension raised `%s` during kill', exc)

        await self.entrypoints.all.kill()
        await self._kill_worker_threads()
        await self.extensions.all.kill()
        await self._kill_managed_threads()

        self.started = False

    async def wait(self):
        """ Block until the container has been stopped.

        If the container was stopped due to an exception, ``wait()`` will
        raise it.

        Any unhandled exception raised in a managed thread or in the
        worker lifecycle (e.g. inside :meth:`DependencyProvider.worker_setup`)
        results in the container being ``kill()``ed, and the exception
        raised from ``wait()``.
        """
        await asyncio.gather(*self._managed_threads.keys())
        return await self._died.wait()

    async def run_worker(self, entrypoint, args, kwargs,
                   context_data=None, handle_result=None):
        worker_ctx = self.spawn_worker(entrypoint, args, kwargs,
                                       context_data=context_data,
                                       handle_result=handle_result)
        task = self._worker_threads.pop(worker_ctx, None)
        if task is not None:
            await task
        return worker_ctx

    def spawn_worker(self, entrypoint, args, kwargs,
                     context_data=None, handle_result=None):
        """ Spawn a worker thread for running the service method decorated
        by `entrypoint`.

        ``args`` and ``kwargs`` are used as parameters for the service method.

        ``context_data`` is used to initialize a ``WorkerContext``.

        ``handle_result`` is an optional function which may be passed
        in by the entrypoint. It is called with the result returned
        or error raised by the service method. If provided it must return a
        value for ``result`` and ``exc_info`` to propagate to dependencies;
        these may be different to those returned by the service method.
        """

        if self._being_killed:
            _log.info("Worker spawn prevented due to being killed")
            raise ContainerBeingKilled()

        service = self.service_cls()
        worker_ctx = WorkerContext(
            self, service, entrypoint, args, kwargs, data=context_data
        )

        async def work():
            try:
                result = await self._run_worker(worker_ctx)
            finally:
                self._worker_threads.pop(worker_ctx, None)

        task = asyncio.create_task(work())
        self._worker_threads[worker_ctx] = task
        return worker_ctx

    def spawn_managed_thread(self, fn, identifier=None):
        """ Spawn a managed thread to run ``fn`` on behalf of an extension.
        The passed `identifier` will be included in logs related to this
        thread, and otherwise defaults to `fn.__name__`, if it is set.

        Any uncaught errors inside ``fn`` cause the container to be killed.

        It is the caller's responsibility to terminate their spawned threads.
        Threads are killed automatically if they are still running after
        all extensions are stopped during :meth:`ServiceContainer.stop`.

        Extensions should delegate all thread spawning to the container.
        """
        if identifier is None:
            identifier = getattr(fn, '__name__', "<unknown>")

        task = asyncio.create_task(fn())
        self._managed_threads[task] = identifier

        def remove_task(f):
            self._managed_threads.pop(task, None)

        task.add_done_callback(remove_task)

        return task

    async def _run_worker(self, worker_ctx, handle_result=None):
        _log.debug('setting up %s', worker_ctx)

        _log.debug('call stack for %s: %s',
                   worker_ctx, '->'.join(worker_ctx.call_id_stack))

        with _log_time('ran worker %s', worker_ctx):

            await self._inject_dependencies(worker_ctx)
            await self._worker_setup(worker_ctx)

            result = exc_info = None
            method_name = worker_ctx.entrypoint.method_name
            method = getattr(worker_ctx.service, method_name)
            try:
                _log.debug('calling handler for %s', worker_ctx)

                with _log_time('ran handler for %s', worker_ctx):
                    result = await method(*worker_ctx.args, **worker_ctx.kwargs)
            except Exception as exc:
                if isinstance(exc, worker_ctx.entrypoint.expected_exceptions):
                    _log.warning(
                        '(expected) error handling worker %s: %s',
                        worker_ctx, exc, exc_info=True)
                else:
                    _log.exception(
                        'error handling worker %s: %s', worker_ctx, exc)
                exc_info = sys.exc_info()

            if handle_result is not None:
                _log.debug('handling result for %s', worker_ctx)

                with _log_time('handled result for %s', worker_ctx):
                    result, exc_info = await handle_result(worker_ctx, result, exc_info)

            with _log_time('tore down worker %s', worker_ctx):

                await self._worker_result(worker_ctx, result, exc_info)

                # we don't need this any more, and breaking the cycle means
                # this can be reclaimed immediately, rather than waiting for a
                # gc sweep
                del exc_info

                await self._worker_teardown(worker_ctx)


    async def _inject_dependencies(self, worker_ctx):
        for provider in self.dependencies:
            dependency = await provider.get_dependency(worker_ctx)
            setattr(worker_ctx.service, provider.attr_name, dependency)

    async def _worker_setup(self, worker_ctx):
        for provider in self.dependencies:
            await provider.worker_setup(worker_ctx)

    async def _worker_result(self, worker_ctx, result, exc_info):
        _log.debug('signalling result for %s', worker_ctx)
        for provider in self.dependencies:
            await provider.worker_result(worker_ctx, result, exc_info)

    async def _worker_teardown(self, worker_ctx):
        for provider in self.dependencies:
            await provider.worker_teardown(worker_ctx)

    async def _kill_worker_threads(self):
        """ Kill any currently executing worker threads.

        See :meth:`ServiceContainer.spawn_worker`
        """
        num_workers = len(self._worker_threads)

        if num_workers:
            _log.warning('killing %s active workers(s)', num_workers)
            worker_items = list(self._worker_threads.items())
            for worker_ctx, task in worker_items:
                _log.warning('killing active worker for %s', worker_ctx)
                task.cancel()
            await asyncio.gather(task for _, task in worker_items)

    async def _kill_managed_threads(self):
        """ Kill any currently executing managed threads.

        See :meth:`ServiceContainer.spawn_managed_thread`
        """
        num_threads = len(self._managed_threads)

        if num_threads:
            _log.warning('killing %s managed thread(s)', num_threads)
            managed_items = list(self._managed_threads.items())
            for task, identifier in managed_items:
                _log.warning('killing managed thread `%s`', identifier)
                task.cancel()
            await asyncio.gather(task for task, _ in worker_items)

    def __repr__(self):
        service_name = self.service_name
        return '<ServiceContainer [{}] at 0x{:x}>'.format(
            service_name, id(self))

    # @contextmanager
    # def _handle_task_exited(self):
    #     try:
    #         yield
    #     except asyncio.CancelledError:
    #         # we don't care much about threads killed by the container
    #         # this can happen in stop() and kill() if extensions
    #         # don't properly take care of their threads
    #         _log.debug('%s thread killed by container', self)
    #     except Exception as e:
    #         _log.critical('%s thread exited with error', self, exc_info=True)
    #         # any uncaught error in a thread is unexpected behavior
    #         # and probably a bug in the extension or container.
    #         # to be safe we call self.kill() to kill our dependencies and
    #         # provide the exception info to be raised in self.wait().
    #         self.kill(sys.exc_info())
