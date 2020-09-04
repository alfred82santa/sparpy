import os
import subprocess
import sys
from itertools import chain
from logging import Logger, getLogger
from pathlib import Path
from typing import Iterable, Mapping, Union

from .config import ConfigParser


class SparkSubmitCommand:

    def __init__(self,
                 config: Mapping = None,
                 spark_executable: str = None,
                 master: str = None,
                 deploy_mode: str = None,
                 conf: Iterable[str] = None,
                 packages: Union[Iterable[str], str] = None,
                 repositories: Union[Iterable[str], str] = None,
                 reqs_paths: Union[Iterable[str], str] = None,
                 logger: Logger = None):
        try:
            config = config['spark']
        except (KeyError, TypeError):
            config = ConfigParser(default_sections=('spark',))
            config = config['spark']

        self.spark_executable = spark_executable or config.get('spark-executable', fallback='spark-submit')
        self.master = master or config.get('master', fallback='local')
        self.deploy_mode = deploy_mode or config.get('deploy-mode', fallback='client')
        self.conf = config.getlist('conf', fallback=[])
        self.packages = config.getlist('packages', fallback=[])
        self.repositories = config.getlist('repositories', fallback=[])
        self.reqs_paths = config.getlist('reqs_paths', fallback=[])

        self.logger = logger or getLogger(__name__)

        if conf:
            self.conf.extend(conf)

        if packages:
            if isinstance(packages, str):
                packages = packages.split(',')

            packages = [p.strip() for p in packages if p.strip()]
            if packages:
                self.packages.extend(packages)

        if repositories:
            if isinstance(repositories, str):
                repositories = repositories.split(',')

            repositories = [p.strip() for p in repositories if p.strip()]
            if repositories:
                self.repositories.extend(repositories)

        if reqs_paths:
            if isinstance(reqs_paths, str):
                reqs_paths = reqs_paths.split(',')
            reqs_paths = [Path(p.strip()) for p in reqs_paths if p.strip()]

            if reqs_paths:
                self.reqs_paths.extend(reqs_paths)

    def build_command(self, job_args: Iterable[str]):
        spark_cmd = [self.spark_executable, '--master', self.master, '--deploy-mode', self.deploy_mode]
        if self.conf:
            spark_cmd.extend(chain(*[['--conf', c] for c in self.conf]))

        if self.packages:
            spark_cmd.extend(['--packages', ','.join(self.packages)])

        if self.repositories:
            spark_cmd.extend(['--repositories', ','.join(self.repositories)])

        if self.reqs_paths:
            ps = [r.strip() for r in chain(*[[str(p.resolve())
                                              for p in chain(Path(rp).rglob('*.egg'),
                                                             Path(rp).rglob('*.whl'),
                                                             Path(rp).rglob('*.zip'))
                                              if p.is_file()] for rp in self.reqs_paths]) if r.strip()]

            if len(ps):
                spark_cmd.extend(['--py-files', ','.join(ps)])

        spark_cmd.extend(job_args)

        return spark_cmd

    def run(self, job_args: Iterable[str]):
        self.logger.info('Executing Spark job...')
        spark_command = self.build_command(job_args=job_args)

        env = os.environ.copy()
        env['PYSPARK_PYTHON'] = sys.executable

        self.logger.info(' '.join(spark_command))
        result = subprocess.check_call(spark_command, stdout=sys.stdout, stderr=sys.stderr, env=env)
        if result:
            self.logger.error(f'Spark job failed with error: {result}')
            raise RuntimeError()
