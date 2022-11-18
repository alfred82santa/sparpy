from configparser import ConfigParser
from functools import update_wrapper
from pathlib import Path

import click

from . import __version__
from .config import load_user_config


class EnvValue(click.types.StringParamType):
    name = 'env_var=value'

    def convert(self, value, param, ctx):
        if isinstance(value, tuple):
            return value

        value = super(EnvValue, self).convert(value, param, ctx)
        value = tuple(value.split('=', 1))
        if len(value) == 1:
            value = (value[0], '')
        return value

    def __repr__(self):
        return "ENV_VAR=VALUE"


def apply_decorators(func, *args):
    fn = func
    for opt in args:
        fn = opt(fn)

    return update_wrapper(fn, func)


def plugins_options(func=None):
    def inner(fn):
        return apply_decorators(
            fn,
            click.option(
                '--plugin', '-p',
                type=str,
                multiple=True,
                envvar='SPARPY_PLUGINS',
                help='Download plugin'
            ),
            click.option(
                '--requirements-file', '-r',
                type=click.Path(),
                multiple=True,
                envvar='SPARPY_REQUIREMENT_FILES',
                help='Plugins requirements file'
            ),
            click.option(
                '--constraint', '-c',
                type=click.Path(),
                multiple=True,
                envvar='SPARPY_CONSTRAINTS',
                help='Package constraints'
            ),
            click.option(
                '--exclude-python-package', '-x',
                type=click.Path(),
                multiple=True,
                envvar='SPARPY_EXCLUDE_PYTHON_PACKAGES',
                help='Exclude Python packages'
            ),
            click.option(
                '--no-index',
                is_flag=True,
                type=bool,
                default=None,
                envvar='SPARPY_NO_INDEX',
                help='Ignore package index (only looking at --find-links URLs instead).'
            ),
            click.option(
                '--extra-index-url', '-e',
                type=str,
                multiple=True,
                envvar='SPARPY_EXTRA_INDEX_URLS',
                help='Extra repository url'
            ),
            click.option(
                '--find-links', '-f',
                type=str,
                multiple=True,
                envvar='SPARPY_FIND_LINKS',
                help='If a URL or path to an html file, then parse for links to archives such as sdist (.tar.gz)'
                     ' or wheel (.whl) files. If a local path or file:// URL that\'s a directory,  then look for'
                     ' archives in the directory listing. Links to VCS project URLs are not supported.'
            ),
            click.option(
                '--no-self',
                is_flag=True,
                type=bool,
                default=None,
                envvar='SPARPY_NO_SELF',
                help='No include Sparpy itself as requirement'
            ),
            click.option(
                '--force-download',
                is_flag=True,
                type=bool,
                default=None,
                envvar='SPARPY_FORCE_DOWNLOAD',
                help='Avoid cache and download all packages'
            ),
            click.option(
                '--pre',
                is_flag=True,
                type=bool,
                default=None,
                envvar='SPARPY_PRE_RELEASES',
                help='Include pre-release and development versions. By default, sparpy only finds stable versions.'
            ),
            click.option(
                '--plugin-env',
                type=EnvValue(),
                multiple=True,
                envvar='SPARPY_PLUGIN_ENVVARS',
                help='Environment variables values for plugin download process'
            ),
        )

    if func:
        return inner(func)
    return inner


def common_spark_options(func=None):
    def inner(fn):
        return apply_decorators(
            fn,
            click.option(
                '--master',
                type=str,
                envvar='SPARPY_MASTER',
                help='The master URL for the cluster'
            ),
            click.option(
                '--deploy-mode',
                type=str,
                envvar='SPARPY_DEPLOY_MODE',
                help='Whether to deploy your driver on the worker nodes (cluster) '
                     'or locally as an external client (client)'
            ),
            click.option(
                '--queue',
                type=str,
                envvar='SPARPY_QUEUE',
                help='The name of the YARN queue to which the application is submitted.'
            ),
            click.option(
                '--conf',
                type=str,
                multiple=True,
                envvar='SPARPY_CONF',
                help='Arbitrary Spark configuration property in key=value format. '
                     'For values that contain spaces wrap “key=value” in quotes.'
            ),
            click.option(
                '--packages',
                type=str,
                envvar='SPARPY_PACKAGES',
                help='Comma-delimited list of Maven coordinates'
            ),
            click.option(
                '--exclude-packages',
                type=str,
                envvar='SPARPY_EXCLUDE_PACKAGES',
                help='Comma-delimited list of Maven coordinates'
            ),
            click.option(
                '--repositories',
                type=str,
                envvar='SPARPY_REPOSITORIES',
                help='Comma-delimited list of Maven repositories'
            ),
            click.option(
                '--env',
                type=EnvValue(),
                multiple=True,
                envvar='SPARPY_ENVVARS',
                help='Environment variables values'
            ),
            click.option(
                '--properties-file',
                type=str,
                envvar='SPARPY_PROPERTIES_FILE',
                help="Path to a file from which to load extra properties. If not"
                     "specified, this will look for conf/spark-defaults.conf."
            ),
            click.option(
                '--klass', '--class',
                type=str,
                envvar='SPARPY_CLASS',
                help='Your application\'s main class (for Java / Scala apps).'
            ),
        )

    if func:
        return inner(func)
    return inner


def spark_submit_options(func=None):
    def inner(fn):
        return apply_decorators(
            fn,
            click.option(
                '--spark-submit-executable',
                type=str,
                help='Spark submit executable'
            ),
            click.argument(
                'job_args',
                nargs=-1,
                type=click.UNPROCESSED
            )
        )

    if func:
        return inner(func)
    return inner


def spark_interactive_options(func=None):
    def inner(fn):
        return apply_decorators(
            fn,
            click.option(
                '--pyspark-executable',
                type=str,
                help='PySpark executable'
            ),
            click.option(
                '--python-interactive-driver',
                type=str,
                help='Python interactive driver'
            )
        )

    if func:
        return inner(func)
    return inner


class Config(click.ParamType):
    name = 'configfile'

    def __call__(self, value, param=None, ctx=None):
        return self.convert(value, param, ctx)

    def convert(self, value, param, ctx):
        if isinstance(value, ConfigParser):
            return value

        if value:
            value = Path(value)
        return load_user_config(value)


def general_options(func=None):
    def inner(fn):
        return apply_decorators(
            fn,
            click.option(
                '--config',
                type=Config(),
                default=load_user_config(),
                envvar='SPARPY_CONFIG',
                help='Path to configuration file'
            ),
            click.option(
                '--debug', '-d',
                type=bool,
                default=False,
                is_flag=True,
                envvar='SPARPY_DEBUG',
                help='Debug mode'
            ),
            click.version_option(version=__version__)
        )

    if func:
        return inner(func)
    return inner
