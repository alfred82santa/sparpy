from itertools import chain
from pathlib import Path


def build_spark_submit_command(spark_executable='spark-submit',
                               master='local',
                               deploy_mode='client',
                               conf=None,
                               packages=None,
                               repositories=None,
                               reqs_path=None,
                               job_args=tuple()):
    spark_cmd = [spark_executable, '--master', master, '--deploy-mode', deploy_mode]
    if conf:
        spark_cmd.extend(chain(*[['--conf', c] for c in conf]))

    if packages:
        spark_cmd.extend(['--packages', packages])

    if repositories:
        spark_cmd.extend(['--repositories', repositories])

    if reqs_path:
        spark_cmd.extend(['--py-files', ','.join([str(p.resolve())
                                                  for p in Path(reqs_path).glob('*.zip')
                                                  if p.is_file()])])

    spark_cmd.extend(job_args)

    return spark_cmd
