from pathlib import Path
from shutil import rmtree

import click

from sparpy.cli_options import (common_spark_options, general_options,
                                plugins_options, spark_interactive_options,
                                spark_submit_options)
from sparpy.logger import build_logger
from sparpy.plugins import DownloadPlugins, DynamicGroup
from sparpy.spark import SparkInteractiveCommand, SparkSubmitCommand


@click.group(cls=DynamicGroup)
def sparpy_runner():
    pass


def run_sparpy_runner():
    sparpy_runner(obj={})


@click.command(name='sparpy', context_settings=dict(ignore_unknown_options=True))
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


@click.command(name='sparpy-download', context_settings=dict(ignore_unknown_options=True))
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
def sparpy_download(config,
                    debug,
                    # Plugin options
                    plugin,
                    requirements_file,
                    extra_index_url,
                    find_links,
                    no_index,
                    no_self,
                    force_download,
                    # Output
                    convert_to_zip,
                    output_dir,
                    *,
                    logger=None):
    """
    Download all dependencies and store them in a directory
    """

    logger = logger or build_logger(config, debug)

    reqs_path = DownloadPlugins(config=config,
                                plugins=plugin,
                                requirements_files=requirements_file,
                                extra_index_urls=extra_index_url,
                                find_links=find_links,
                                no_index=no_index,
                                no_self=no_self,
                                force_download=force_download,
                                logger=logger,
                                convert_to_zip=convert_to_zip,
                                download_dir=output_dir).download(debug=debug)

    click.echo(f'Packages directory: {reqs_path}')
    return reqs_path


def run_sparpy_download():
    sparpy_download(obj={})


@click.command(name='sparpy-submit', context_settings=dict(ignore_unknown_options=True))
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
                  extra_index_url,
                  find_links,
                  no_index,
                  no_self,
                  force_download,
                  # Spark submit options
                  spark_submit_executable,
                  # Common Spark options
                  master,
                  deploy_mode,
                  queue,
                  conf,
                  packages,
                  repositories,
                  # Job arguments
                  job_args,
                  *,
                  logger=None):
    """
    Submit an spark job defined on an script
    """

    logger = logger or build_logger(config, debug)

    reqs_path = ctx.invoke(sparpy_download,
                           config=config,
                           debug=debug,
                           plugin=plugin,
                           requirements_file=requirements_file,
                           extra_index_url=extra_index_url,
                           find_links=find_links,
                           no_index=no_index,
                           no_self=no_self,
                           force_download=force_download,
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
                                       repositories=repositories,
                                       reqs_paths=reqs_paths,
                                       logger=logger)

    try:
        spark_command.run(job_args=job_args)
    finally:
        if reqs_path:
            rmtree(reqs_path)


def run_sparpy_submit():
    sparpy(obj={})


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
            extra_index_url,
            find_links,
            no_index,
            no_self,
            force_download,
            # Spark interactive options
            pyspark_executable,
            python_interactive_driver,
            # Common Spark options
            master,
            deploy_mode,
            queue,
            conf,
            packages,
            repositories,
            *,
            logger=None):
    """
    Start a pyspark interactive session with dependencies loaded
    """

    logger = logger or build_logger(config, debug)

    reqs_path = ctx.invoke(sparpy_download,
                           debug=debug,
                           config=config,
                           plugin=plugin,
                           requirements_file=requirements_file,
                           extra_index_url=extra_index_url,
                           find_links=find_links,
                           no_index=no_index,
                           no_self=no_self,
                           force_download=force_download,
                           convert_to_zip=True,
                           logger=logger)

    reqs_paths = []
    if reqs_path is not None:
        reqs_paths.append(reqs_path)

    spark_command = SparkInteractiveCommand(config=config,
                                            pyspark_executable=pyspark_executable,
                                            python_interactive_driver=python_interactive_driver,
                                            master=master,
                                            deploy_mode=deploy_mode,
                                            queue=queue,
                                            conf=conf,
                                            packages=packages,
                                            repositories=repositories,
                                            reqs_paths=reqs_paths,
                                            logger=logger)

    try:
        spark_command.run()
    finally:
        if reqs_path:
            rmtree(reqs_path)


def run_isparpy():
    isparpy(obj={})
