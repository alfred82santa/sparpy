from functools import update_wrapper
from pathlib import Path

import click

from sparpy.config import load_user_config


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
                help='Download plugin'
            ),
            click.option(
                '--requirements-file', '-r',
                type=click.Path(),
                multiple=True,
                help='Plugins requirements file'
            ),
            click.option(
                '--extra-index-url', '-e',
                type=str,
                multiple=True,
                help='Extra repository url'
            ),
            click.option(
                '--no-self',
                is_flag=True,
                type=bool,
                default=False,
                help='No include Sparpy itself as requirement'
            )
        )

    if func:
        return inner(func)
    return inner


def spark_options(func=None):
    def inner(fn):
        return apply_decorators(
            fn,
            click.option(
                '--spark-submit-executable',
                type=str,
                help='Spark submit executable'
            ),
            click.option(
                '--master',
                type=str,
                help='The master URL for the cluster'
            ),
            click.option(
                '--deploy-mode',
                type=str,
                help='Whether to deploy your driver on the worker nodes (cluster) '
                     'or locally as an external client (client)'
            ),
            click.option(
                '--conf',
                type=str,
                multiple=True,
                help='Arbitrary Spark configuration property in key=value format. '
                     'For values that contain spaces wrap “key=value” in quotes.'
            ),
            click.option(
                '--packages',
                type=str,
                help='Comma-delimited list of Maven coordinates'
            ),
            click.option(
                '--repositories',
                type=str,
                help='Comma-delimited list of Maven repositories'
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


class Config(click.ParamType):
    name = 'configfile'

    def __call__(self, value, param=None, ctx=None):
        return self.convert(value, param, ctx)

    def convert(self, value, param, ctx):
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
                help='Path to configuration file'
            ),
            click.option(
                '--debug', '-d',
                type=bool,
                default=False,
                is_flag=True,
                help='Debug mode'
            )
        )

    if func:
        return inner(func)
    return inner
