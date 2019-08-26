import asyncio
from functools import partial
import signal
import sys

from contextlib import asynccontextmanager
from logging import getLogger

from nameko.cli.utils import import_services
from nameko.asynced.containers import SpawningProxy, get_container_cls, get_service_name


_log = getLogger(__name__)


class ServiceRunner:
    """ Allows the user to serve a number of services concurrently.
    The caller can register a number of service classes with a name and
    then use the start method to serve them and the stop and kill methods
    to stop them. The wait method will block until all services have stopped.

    Example::

        runner = ServiceRunner()
        runner.add_service(Foobar)
        runner.add_service(Spam)

        add_sig_term_handler(runner.kill)

        runner.start()

        runner.wait()
    """
    def __init__(self):
        self.service_map = {}

        self.container_cls = get_container_cls()

    @property
    def service_names(self):
        return self.service_map.keys()

    @property
    def containers(self):
        return self.service_map.values()

    def add_service(self, cls):
        """ Add a service class to the runner.
        There can only be one service class for a given service name.
        Service classes must be registered before calling start()
        """
        service_name = get_service_name(cls)
        container = self.container_cls(cls)
        self.service_map[service_name] = container

    async def start(self):
        """ Start all the registered services.

        A new container is created for each service using the container
        class provided in the __init__ method.

        All containers are started concurrently and the method will block
        until all have completed their startup routine.
        """
        service_names = ', '.join(self.service_names)
        _log.info('starting services: %s', service_names)

        await SpawningProxy(self.containers).start()

        _log.debug('services started: %s', service_names)

    async def stop(self):
        """ Stop all running containers concurrently.
        The method blocks until all containers have stopped.
        """
        service_names = ', '.join(self.service_names)
        _log.info('stopping services: %s', service_names)

        await SpawningProxy(self.containers).stop()

        _log.debug('services stopped: %s', service_names)

    async def kill(self):
        """ Kill all running containers concurrently.
        The method will block until all containers have stopped.
        """
        service_names = ', '.join(self.service_names)
        _log.info('killing services: %s', service_names)

        await SpawningProxy(self.containers).kill()

        _log.debug('services killed: %s ', service_names)

    async def wait(self):
        """ Wait for all running containers to stop.
        """
        try:
            await SpawningProxy(self.containers, abort_on_error=True).wait()
        except Exception:
            # If a single container failed, stop its peers and re-raise the
            # exception
            _log.debug('stopping runner')
            await self.stop()
            raise


@asynccontextmanager
async def run_services(*services, **kwargs):
    """ Serves a number of services for a contextual block.
    The caller can specify a number of service classes then serve them either
    stopping (default) or killing them on exiting the contextual block.


    Example::

        with run_services(Foobar, Spam) as runner:
            # interact with services and stop them on exiting the block

        # services stopped


    Additional configuration available to :class:``ServiceRunner`` instances
    can be specified through keyword arguments::

        with run_services(Foobar, Spam, kill_on_exit=True):
            # interact with services

        # services killed

    :Parameters:
        services : service definitions
            Services to be served for the contextual block
        kill_on_exit : bool (default=False)
            If ``True``, run ``kill()`` on the service containers when exiting
            the contextual block. Otherwise ``stop()`` will be called on the
            service containers on exiting the block.

    :Returns: The configured :class:`ServiceRunner` instance

    """
    kill_on_exit = kwargs.pop('kill_on_exit', False)

    runner = ServiceRunner()
    for service in services:
        runner.add_service(service)

    await runner.start()

    yield runner

    if kill_on_exit:
        await runner.kill()
    else:
        await runner.stop()


async def run(service_modules):
    if "." not in sys.path:
        sys.path.append(".")

    services = []
    for path in service_modules:
        services.extend(import_services(path))

    service_runner = ServiceRunner()
    for service_cls in services:
        service_runner.add_service(service_cls)

    loop = asyncio.get_event_loop()

    def handle_signal(signal, f):
        print()
        _log.warning(f'handling signal {signal}')
        asyncio.create_task(f())

    loop.add_signal_handler(
        signal.SIGTERM, partial(handle_signal, signal.SIGTERM, service_runner.stop)
    )

    # loop.add_signal_handler(
    #     signal.SIGINT, partial(handle_signal, signal.SIGINT, service_runner.kill)
    # )

    await service_runner.start()
    try:
        await service_runner.wait()
    except asyncio.CancelledError:
        _log.warning('service shut down.')
    except KeyboardInterrupt:
        print()  # looks nicer with the ^C e.g. bash prints in the terminal
        try:
            await service_runner.stop()
        except KeyboardInterrupt:
            print()  # as above
            await service_runner.kill()
        finally:
            await service_runner.wait()



def main(services, backdoor_port):
    # sys.path already manipulated at services import time
    if "LOGGING" in config:
        logging.config.dictConfig(config["LOGGING"])
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


    run(services, backdoor_port=backdoor_port)
