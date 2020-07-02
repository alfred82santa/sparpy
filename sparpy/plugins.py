import subprocess
import sys
from itertools import chain
from pathlib import Path
from pkgutil import iter_modules
from tempfile import mkdtemp
from urllib.parse import urlparse
from zipimport import zipimporter

from click import Group, echo
from pkg_resources import find_distributions, iter_entry_points, working_set


class DynamicGroup(Group):
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

        return plugin.load()


def ensure_plugin_distribution():
    for module_info in iter_modules():
        if not isinstance(module_info.module_finder, zipimporter):
            continue

        for dist in find_distributions(module_info.module_finder.archive):
            working_set.add(dist)


def convert_to_zip(f: Path):
    new_path = f.with_suffix('.zip')
    f.rename(new_path)
    return new_path


def download_plugins(plugins=None, requirements_files=None, extra_index_urls=None, no_self=False, force_download=False):
    plugins = plugins or []
    requirements_files = requirements_files or []
    extra_index_urls = extra_index_urls or []

    if no_self and not len(plugins) and not len(requirements_files):
        return None

    pip_exec_params = [sys.executable, '-m', 'pip', 'download']
    reqs_path = mkdtemp(prefix='sparpy_')
    pip_exec_params.extend(['-d', reqs_path])
    pip_exec_params.extend(chain(*[['--extra-index-url',
                                    u,
                                    '--trusted-host',
                                    urlparse(u).hostname]
                                   for u in extra_index_urls]))
    if not no_self:
        from . import __version__
        pip_exec_params.append(f'sparpy={__version__}')
    if len(plugins):
        pip_exec_params.extend(chain(*[p.split(',') if ',' in p else [p, ] for p in plugins]))
    if len(requirements_files):
        pip_exec_params.extend(chain(*[['-r', r] for r in requirements_files]))

    pip_exec_params.extend(['--exists-action', 'i'])

    echo('Downloading python plugins...')
    echo(' '.join(pip_exec_params))
    subprocess.check_call(pip_exec_params, stdout=sys.stdout, stderr=sys.stderr)

    [convert_to_zip(p.resolve())
     for p in Path(reqs_path).glob('*.whl')
     if p.is_file()]

    return reqs_path
