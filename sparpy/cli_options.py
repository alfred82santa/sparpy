import click


def apply_decorators(func, *args):
    for opt in args:
        func = opt(func)

    return func


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
                default=('http://artifactory.hi.inet/artifactory/api/pypi/pypi-phb-ctb/simple',),
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
                '--master',
                type=str,
                default='yarn',
                help='The master URL for the cluster'
            ),
            click.option(
                '--deploy-mode',
                type=str,
                default='client',
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
