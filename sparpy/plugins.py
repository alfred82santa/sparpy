import sys
from itertools import chain
from logging import Logger, getLogger
from pathlib import Path
from pkgutil import iter_modules
from tempfile import mkdtemp
from typing import Iterable
from urllib.parse import urlparse
from zipimport import zipimporter

import click
from pkg_resources import find_distributions, iter_entry_points, working_set

from .config import ConfigParser
from .processor import ProcessManager


class DynamicGroup(click.Group):
    PLUGINS_ENTRY_POINT = 'sparpy.cli_plugins'

    def __init__(self, *args, **kwargs):
        super(DynamicGroup, self).__init__(*args, **kwargs)

        ensure_plugin_distribution()

    def iter_plugins(self):
        yield from iter_entry_points(self.PLUGINS_ENTRY_POINT)

    def get_plugin(self, name):
        for plugin in self.iter_plugins():
            if plugin.name == name:
                return plugin

        raise RuntimeError(f'Plugin {name} does not exist')

    def list_commands(self, ctx):
        rv = sorted([plugin.name for plugin in self.iter_plugins()])
        return rv

    def get_command(self, ctx, name):
        try:
            plugin = self.get_plugin(name=name)
        except IndexError:
            return None

        command = plugin.resolve()

        if isinstance(command, click.BaseCommand):
            return command

        @click.command(name=name, context_settings={'ignore_unknown_options': True})
        @click.argument(
            'job_args',
            nargs=-1,
            type=click.UNPROCESSED
        )
        def wrapper(job_args):
            ctx.exit(command(job_args))

        return wrapper


def ensure_plugin_distribution():
    for module_info in iter_modules():
        if not isinstance(module_info.module_finder, zipimporter):
            continue

        for dist in find_distributions(module_info.module_finder.archive):
            working_set.add(dist)


def _convert_to_zip(f: Path):
    new_path = f.with_suffix('.zip')
    f.rename(new_path)
    return new_path


class DownloadPlugins:

    def __init__(self,
                 config=None,
                 plugins: Iterable[str] = None,
                 requirements_files: Iterable[str] = None,
                 constraints: Iterable[str] = None,
                 extra_index_urls: Iterable[str] = None,
                 find_links: Iterable[str] = None,
                 no_index: bool = None,
                 no_self: bool = None,
                 force_download: bool = None,
                 pre: bool = None,
                 logger: Logger = None,
                 download_dir: str = None,
                 convert_to_zip: bool = True):
        try:
            config = config['plugins']
        except (KeyError, TypeError):
            config = ConfigParser(default_sections=('plugins',))
            config = config['plugins']

        self.logger = logger or getLogger(__name__)

        self.extra_index_urls = config.getlist('extra-index-urls', fallback=[])
        self.find_links = config.getlist('find-links', fallback=[])
        self.no_index = config.getboolean('no-index', fallback=False)
        self.cache_dir = config.getpath('cache-dir')

        self.plugins = config.getlist('plugins', fallback=[])
        self.requirements_files = config.getpathlist('requirements-files', fallback=[])
        self.constraints = config.getpathlist('constraints', fallback=[])

        self.download_dir_prefix = config.get('download-dir-prefix', fallback='sparpy_')

        self.no_self = config.getboolean('no-self', fallback=False)
        self.force_download = config.getboolean('force-download', fallback=False)
        self.pre = config.getboolean('pre', fallback=False)

        if plugins:
            self.plugins.extend(plugins)

        if requirements_files:
            self.requirements_files.extend([Path(p) for p in requirements_files])

        if constraints:
            self.constraints.extend([Path(p) for p in constraints])

        if extra_index_urls:
            self.extra_index_urls.extend(extra_index_urls)

        if find_links:
            self.find_links.extend(find_links)

        if no_index is not None:
            self.no_index = no_index

        if no_self is not None:
            self.no_self = no_self

        if force_download is not None:
            self.force_download = force_download

        if pre is not None:
            self.pre = pre

        if download_dir:
            self.reqs_path = download_dir
        else:
            self.reqs_path = mkdtemp(prefix=self.download_dir_prefix)

        self.convert_to_zip = convert_to_zip

    def build_command(self):
        pip_exec_params = [sys.executable, '-m', 'pip', 'download']
        pip_exec_params.extend(['-d', self.reqs_path])

        if self.force_download:
            pip_exec_params.append('--no-cache-dir')
        elif self.cache_dir:
            pip_exec_params.extend(['--cache-dir', str(self.cache_dir)])

        if self.pre:
            pip_exec_params.append('--pre')

        pip_exec_params.extend(chain.from_iterable([['--extra-index-url',
                                                     u,
                                                     '--trusted-host',
                                                     urlparse(u).hostname]
                                                    for u in self.extra_index_urls]))

        pip_exec_params.extend(chain.from_iterable([['--find-links', str(r)] for r in self.find_links]))
        if self.no_index:
            pip_exec_params.append('--no-index')

        if not self.no_self:
            from . import __version__
            pip_exec_params.append(f'sparpy=={__version__}')
        if len(self.plugins):
            pip_exec_params.extend(chain.from_iterable([p.split(',') if ',' in p else [p, ] for p in self.plugins]))
        if len(self.requirements_files):
            pip_exec_params.extend(chain.from_iterable([['-r', str(r)] for r in self.requirements_files]))
        if len(self.constraints):
            pip_exec_params.extend(chain.from_iterable([['-c', str(c)] for c in self.constraints]))

        pip_exec_params.extend(['--exists-action', 'i'])

        return pip_exec_params

    def download(self, debug=False):
        if self.no_self and not len(self.plugins) and not len(self.requirements_files):
            return None

        pip_exec_params = self.build_command()

        self.logger.info('Downloading python plugins...')
        self.logger.debug(' '.join(pip_exec_params))

        process = ProcessManager(pip_exec_params, pass_through=debug)
        process.start_process()
        process.wait()

        if process.returncode != 0:
            raise RuntimeError('Download packages failed')

        if self.convert_to_zip:
            [_convert_to_zip(p.resolve())
             for p in Path(self.reqs_path).glob('*.whl')
             if p.is_file()]

        return self.reqs_path
