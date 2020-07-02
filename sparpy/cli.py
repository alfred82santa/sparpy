import subprocess
import sys
from pathlib import Path
from shutil import rmtree

import click

from sparpy.cli_options import plugins_options, spark_options
from sparpy.plugins import DynamicGroup, download_plugins
from sparpy.spark import build_spark_submit_command


@click.group(cls=DynamicGroup)
def sparpy_runner():
    pass


def run_sparpy_runner():
    sparpy_runner(obj={})


@click.command(name='sparpy')
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


@click.command(name='sparpy-submit')
@plugins_options
@spark_options
def sparpy_submit(plugin,
                  requirements_file,
                  extra_index_url,
                  master,
                  deploy_mode,
                  conf,
                  packages,
                  repositories,
                  job_args,
                  no_self):
    reqs_path = download_plugins(plugins=plugin,
                                 requirements_files=requirements_file,
                                 extra_index_urls=extra_index_url,
                                 no_self=no_self)

    spark_exec = build_spark_submit_command(master=master,
                                            deploy_mode=deploy_mode,
                                            conf=conf,
                                            packages=packages,
                                            repositories=repositories,
                                            job_args=job_args)

    click.echo('Executing Spark job...')
    try:
        click.echo(' '.join(spark_exec))
        if subprocess.check_call(spark_exec, stdout=sys.stdout, stderr=sys.stderr):
            raise RuntimeError()
    finally:
        if reqs_path:
            rmtree(reqs_path)


def run_sparpy_submit():
    sparpy(obj={})
