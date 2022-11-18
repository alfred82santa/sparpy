from pathlib import Path
from shutil import rmtree

import click

from .cli_options import (common_spark_options, general_options,
                          plugins_options, spark_interactive_options,
                          spark_submit_options)
from .logger import build_logger
from .plugins import DownloadPlugins, DynamicGroup
from .spark import SparkInteractiveCommand, SparkSubmitCommand


@click.group(cls=DynamicGroup)
def sparpy_runner():
    pass


def run_sparpy_runner():
    sparpy_runner(obj={})


@click.command(name='sparpy', context_settings={'ignore_unknown_options': True})
@general_options
@plugins_options
@common_spark_options
@spark_submit_options
@click.pass_context
def sparpy(ctx,
           job_args,
           **kwargs):
    """
    Submit an spark job defined on an sparpy plugin
    """

    job_args = list(job_args)
    job_args.insert(0, str(Path(__file__).parent / 'run.py'))

    return ctx.invoke(sparpy_submit,
                      job_args=job_args,
                      **kwargs)


def run_sparpy():
    sparpy(obj={})


@click.command(name='sparpy-download')
@general_options
@plugins_options
@click.option('--convert-to-zip', '-z',
              type=bool,
              default=False,
              is_flag=True,
              help='Whether packages should be converted to zip files')
@click.option('--output-dir', '-o',
              type=click.Path(file_okay=False, writable=True, resolve_path=True),
              required=False,
              help='Directory where to download packages. If it is not provided a temporal directory will be created.')
@click.pass_context
def sparpy_download(ctx,
                    config,
                    debug,
                    # Plugin options
                    plugin,
                    requirements_file,
                    constraint,
                    exclude_python_package,
                    extra_index_url,
                    find_links,
                    no_index,
                    no_self,
                    force_download,
                    pre,
                    plugin_env,
                    # Output
                    convert_to_zip,
                    output_dir,
                    *,
                    logger=None):
    """
    Download all dependencies and store them in a directory
    """

    logger = logger or build_logger(config, debug)

    download_command = DownloadPlugins(config=config,
                                       plugins=plugin,
                                       requirements_files=requirements_file,
                                       constraints=constraint,
                                       exclude_packages=exclude_python_package,
                                       extra_index_urls=extra_index_url,
                                       find_links=find_links,
                                       no_index=no_index,
                                       no_self=no_self,
                                       force_download=force_download,
                                       pre=pre,
                                       env=plugin_env,
                                       logger=logger,
                                       convert_to_zip=convert_to_zip,
                                       download_dir=output_dir)
    try:
        reqs_path = download_command.download(debug=debug)
    except RuntimeError as ex:
        click.echo(ex)
        raise ctx.exit(-1)

    click.echo(f'Packages directory: {reqs_path}')
    return reqs_path


def run_sparpy_download():
    sparpy_download(obj={})


@click.command(name='sparpy-submit', context_settings={'ignore_unknown_options': True})
@general_options
@plugins_options
@common_spark_options
@spark_submit_options
@click.pass_context
def sparpy_submit(ctx,
                  config,
                  debug,
                  # Plugin options
                  plugin,
                  requirements_file,
                  constraint,
                  exclude_python_package,
                  extra_index_url,
                  find_links,
                  no_index,
                  no_self,
                  force_download,
                  pre,
                  plugin_env,
                  # Spark submit options
                  spark_submit_executable,
                  # Common Spark options
                  master,
                  deploy_mode,
                  queue,
                  conf,
                  packages,
                  exclude_packages,
                  repositories,
                  env,
                  properties_file,
                  klass,
                  # Job arguments
                  job_args,
                  *,
                  logger=None):
    """
    Submit an spark job defined on an script
    """

    logger = logger or build_logger(config, debug)

    if conf:
        sparpy_conf = [tuple(k.split('=', 1)) for k in conf if k.startswith('sparpy.')]
        conf = [k for k in conf if not k.startswith('sparpy.')]
        if len(sparpy_conf):
            plugin = [*plugin, *[v for k, v in sparpy_conf if k == 'sparpy.plugins']]
            requirements_file = [*requirements_file, *[v for k, v in sparpy_conf if k == 'sparpy.requirements-file']]
            constraint = [*constraint, *[v for k, v in sparpy_conf if k == 'sparpy.constraints']]
            exclude_python_package = [*exclude_python_package, *
                                      [v for k, v in sparpy_conf if k == 'sparpy.exclude-python-packages']]
            extra_index_url = [*extra_index_url, *[v for k, v in sparpy_conf if k == 'sparpy.extra-index-url']]
            find_links = [*find_links, *[v for k, v in sparpy_conf if k == 'sparpy.find-links']]

            try:
                no_index = [v for k, v in sparpy_conf if k == 'sparpy.no-index'][0].lower() not in ['true', '1']
            except (IndexError, AttributeError):
                pass

            try:
                no_self = [v for k, v in sparpy_conf if k == 'sparpy.no-self'][0].lower() not in ['true', '1']
            except (IndexError, AttributeError):
                pass

            try:
                force_download = [v for k, v in sparpy_conf
                                  if k == 'sparpy.force-download'][0].lower() not in ['true', '1']
            except (IndexError, AttributeError):
                pass

            try:
                pre = [v for k, v in sparpy_conf
                       if k == 'sparpy.pre-releases'][0].lower() not in ['true', '1']
            except (IndexError, AttributeError):
                pass

            plugin_env.update(dict([v.split('=', 1) for k, v in sparpy_conf
                                    if k == 'sparpy.plugin-env']))

    reqs_path = ctx.invoke(sparpy_download,
                           config=config,
                           debug=debug,
                           plugin=plugin,
                           requirements_file=requirements_file,
                           constraint=constraint,
                           exclude_python_package=exclude_python_package,
                           extra_index_url=extra_index_url,
                           find_links=find_links,
                           no_index=no_index,
                           no_self=no_self,
                           force_download=force_download,
                           pre=pre,
                           plugin_env=plugin_env,
                           convert_to_zip=True,
                           logger=logger)

    reqs_paths = []
    if reqs_path is not None:
        reqs_paths.append(reqs_path)

    spark_command = SparkSubmitCommand(config=config,
                                       spark_executable=spark_submit_executable,
                                       master=master,
                                       deploy_mode=deploy_mode,
                                       queue=queue,
                                       conf=conf,
                                       packages=packages,
                                       exclude_packages=exclude_packages,
                                       repositories=repositories,
                                       reqs_paths=reqs_paths,
                                       env=dict(env or {}),
                                       properties_file=properties_file,
                                       klass=klass,
                                       logger=logger)

    try:
        spark_command.run(job_args=job_args)
    except RuntimeError as ex:
        click.echo(ex)
        raise ctx.exit(-1)
    finally:
        if reqs_path:
            rmtree(reqs_path)


def run_sparpy_submit():
    sparpy_submit(obj={})


@click.command(name='isparpy')
@general_options
@plugins_options
@common_spark_options
@spark_interactive_options
@click.pass_context
def isparpy(ctx,
            config,
            debug,
            # Plugin options
            plugin,
            requirements_file,
            constraint,
            exclude_python_package,
            extra_index_url,
            find_links,
            no_index,
            no_self,
            force_download,
            pre,
            plugin_env,
            # Spark interactive options
            pyspark_executable,
            python_interactive_driver,
            # Common Spark options
            master,
            deploy_mode,
            queue,
            conf,
            packages,
            exclude_packages,
            repositories,
            env,
            *,
            logger=None,
            **kwargs):
    """
    Start a pyspark interactive session with dependencies loaded
    """

    logger = logger or build_logger(config, debug)

    reqs_path = ctx.invoke(sparpy_download,
                           debug=debug,
                           config=config,
                           plugin=plugin,
                           requirements_file=requirements_file,
                           constraint=constraint,
                           exclude_python_package=exclude_python_package,
                           extra_index_url=extra_index_url,
                           find_links=find_links,
                           no_index=no_index,
                           no_self=no_self,
                           force_download=force_download,
                           pre=pre,
                           plugin_env=plugin_env,
                           convert_to_zip=True,
                           logger=logger)

    reqs_paths = []
    if reqs_path is not None:
        reqs_paths.append(reqs_path)

    spark_command = SparkInteractiveCommand(cmd_config=config,
                                            pyspark_executable=pyspark_executable,
                                            python_interactive_driver=python_interactive_driver,
                                            master=master,
                                            deploy_mode=deploy_mode,
                                            queue=queue,
                                            conf=conf,
                                            packages=packages,
                                            exclude_packages=exclude_packages,
                                            repositories=repositories,
                                            reqs_paths=reqs_paths,
                                            env=dict(env or {}),
                                            logger=logger)

    try:
        spark_command.run()
    except RuntimeError as ex:
        click.echo(ex)
        raise ctx.exit(-1)
    finally:
        if reqs_path:
            rmtree(reqs_path)


def run_isparpy():
    isparpy(obj={})
