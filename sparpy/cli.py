from pathlib import Path
from shutil import rmtree

import click

from sparpy.cli_options import general_options, plugins_options, spark_options
from sparpy.logger import build_logger
from sparpy.plugins import DownloadPlugins, DynamicGroup
from sparpy.spark import SparkSubmitCommand


@click.group(cls=DynamicGroup)
def sparpy_runner():
    pass


def run_sparpy_runner():
    sparpy_runner(obj={})


@click.command(name='sparpy', context_settings=dict(ignore_unknown_options=True))
@general_options
@plugins_options
@spark_options
@click.pass_context
def sparpy(ctx,
           job_args,
           no_self,
           **kwargs):
    job_args = list(job_args)
    job_args.insert(0, str(Path(__file__).parent / 'run.py'))

    return ctx.invoke(sparpy_submit,
                      job_args=job_args,
                      no_self=no_self,
                      **kwargs)


def run_sparpy():
    sparpy(obj={})


@click.command(name='sparpy-submit', context_settings=dict(ignore_unknown_options=True))
@general_options
@plugins_options
@spark_options
def sparpy_submit(config,
                  debug,
                  plugin,
                  requirements_file,
                  extra_index_url,
                  spark_submit_executable,
                  master,
                  deploy_mode,
                  conf,
                  packages,
                  repositories,
                  job_args,
                  no_self,
                  *,
                  logger=None):
    logger = logger or build_logger(config, debug)

    reqs_path = DownloadPlugins(config=config,
                                plugins=plugin,
                                requirements_files=requirements_file,
                                extra_index_urls=extra_index_url,
                                no_self=no_self,
                                logger=logger).download(debug=debug)

    spark_command = SparkSubmitCommand(config=config,
                                       spark_executable=spark_submit_executable,
                                       master=master,
                                       deploy_mode=deploy_mode,
                                       conf=conf,
                                       packages=packages,
                                       repositories=repositories,
                                       reqs_paths=[reqs_path, ],
                                       logger=logger)

    try:
        spark_command.run(job_args=job_args)
    finally:
        if reqs_path:
            rmtree(reqs_path)


def run_sparpy_submit():
    sparpy(obj={})
