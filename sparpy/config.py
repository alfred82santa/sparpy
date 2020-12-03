from configparser import ConfigParser as BaseConfigParser
from os import PathLike
from pathlib import Path
from typing import Iterable


def load_config(filename: PathLike) -> BaseConfigParser:
    config = ConfigParser()
    if len(config.read(filename)) == 0:
        raise RuntimeError(f'Invalid configuration file: {filename}')

    return config


def load_user_config(filename: PathLike = None) -> BaseConfigParser:
    if not filename:
        filename = Path.home() / '.sparpyrc'
        if not filename.is_file():
            filename = Path('/etc/sparpy.conf')
            if not filename.is_file():
                return ConfigParser()
    return load_config(filename)


class ConfigParser(BaseConfigParser):

    def __init__(self, *args, default_sections: Iterable[str] = None, **kwargs):
        kwargs.setdefault('converters', {})
        kwargs['converters']['list'] = lambda item: [s.strip() for s in item.split('\n') if s.strip()]
        kwargs['converters']['path'] = lambda item: Path(item)
        kwargs['converters']['pathlist'] = lambda item: [Path(s.strip()) for s in item.split('\n') if s.strip()]

        super(ConfigParser, self).__init__(*args, **kwargs)

        if default_sections:
            for section in default_sections:
                if not self.has_section(section):
                    self.add_section(section)

        self.optionxform = lambda option: option
