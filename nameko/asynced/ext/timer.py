import asyncio
import itertools
import time
from logging import getLogger

from nameko.asynced.extensions import Entrypoint


_log = getLogger(__name__)


class Timer(Entrypoint):
    def __init__(self, interval, eager=False, **kwargs):
        """
        Timer entrypoint. Fires every `interval` seconds or as soon as
        the previous worker completes if that took longer.

        The default behaviour is to wait `interval` seconds
        before firing for the first time. If you want the entrypoint
        to fire as soon as the service starts, pass `eager=True`.

        Example::

            timer = Timer.decorator

            class Service(object):
                name = "service"

                @timer(interval=5)
                def tick(self):
                    pass

        """
        self.interval = interval
        self.eager = eager
        self.should_stop = asyncio.Event()
        self.task = None
        super(Timer, self).__init__(**kwargs)

    async def start(self):
        _log.debug('starting %s', self)
        self.task = self.container.spawn_managed_thread(self._run)

    async def stop(self):
        _log.debug('stopping %s', self)
        self.should_stop.set()
        await self.task

    async def kill(self):
        _log.debug('killing %s', self)
        self.task.cancel()
        await self.task

    async def _run(self):
        """ Runs the interval loop. """
        def intervals():
            start_time = time.time()
            start = 0 if self.eager else 1
            for count in itertools.count(start=start):
                yield max(start_time + count * self.interval - time.time(), 0)

        for sleep_time in intervals():
            try:
                await asyncio.wait_for(self.should_stop.wait(), timeout=sleep_time)
            except asyncio.TimeoutError:
                pass
            else:
                break
            _log.debug('tick')
            await self.handle_timer_tick()

    async def handle_timer_tick(self):
        _log.debug('running worker')
        args = ()
        kwargs = {}
        await self.container.run_worker(self, args, kwargs)


timer = Timer.decorator
