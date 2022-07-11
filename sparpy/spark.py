import os
import sys
from itertools import chain
from logging import Logger, getLogger
from pathlib import Path
from typing import Dict, Iterable, Union

from .config import ConfigParser
from .processor import ProcessManager


class BaseSparkCommand:

    def __init__(self,
                 config: ConfigParser = None,
                 spark_executable: str = None,
                 master: str = None,
                 deploy_mode: str = None,
                 queue: str = None,
                 conf: Iterable[str] = None,
                 packages: Union[Iterable[str], str] = None,
                 exclude_packages: Union[Iterable[str], str] = None,
                 repositories: Union[Iterable[str], str] = None,
                 reqs_paths: Union[Iterable[str], str] = None,
                 env: Dict[str, str] = None,
                 properties_file: str = None,
                 klass: str = None,
                 logger: Logger = None):
        try:
            cmd_config = config['spark']
        except KeyError:
            config.add_section('spark')
            cmd_config = config['spark']
        except TypeError:
            config = ConfigParser(default_sections=('spark', 'spark_env'))
            cmd_config = config['spark']

        try:
            env_config = config['spark-env']
        except KeyError:
            config.add_section('spark-env')
            env_config = config['spark-env']

        self.spark_executable = spark_executable or cmd_config.get('spark-executable', fallback='spark-submit')
        self.master = master or cmd_config.get('master')
        self.deploy_mode = deploy_mode or cmd_config.get('deploy-mode')
        self.queue = queue or cmd_config.get('queue')
        self.conf = cmd_config.getlist('conf', fallback=[])
        self.packages = cmd_config.getlist('packages', fallback=[])
        self.exclude_packages = cmd_config.getlist('exclude-packages', fallback=[])
        self.repositories = cmd_config.getlist('repositories', fallback=[])
        self.reqs_paths = cmd_config.getlist('reqs_paths', fallback=[])

        self.property_file = properties_file or cmd_config.get('property-file')
        self.klass = klass or cmd_config.get('class')

        try:
            self.env = {k: v for k, v in env_config.items()}
        except KeyError:
            self.env = {}

        self.env.update(env or {})

        self.logger = logger or getLogger(__name__)

        if conf:
            self.conf.extend(conf)

        if packages:
            if isinstance(packages, str):
                packages = packages.split(',')

            packages = [p.strip() for p in packages if p.strip()]
            if packages:
                self.packages.extend(packages)

        if exclude_packages:
            if isinstance(exclude_packages, str):
                exclude_packages = exclude_packages.split(',')

            exclude_packages = [p.strip() for p in exclude_packages if p.strip()]
            if packages:
                self.exclude_packages.extend(exclude_packages)

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

    def build_command(self, *, executable):
        spark_cmd = [executable, ]
        if self.master:
            spark_cmd.extend(['--master', self.master])

        if self.deploy_mode:
            spark_cmd.extend(['--deploy-mode', self.deploy_mode])

        if self.queue:
            spark_cmd.extend(['--queue', self.queue])

        if self.conf:
            spark_cmd.extend(chain(*[['--conf', c] for c in self.conf]))

        if self.packages:
            spark_cmd.extend(['--packages', ','.join(self.packages)])

        if self.exclude_packages:
            spark_cmd.extend(['--exclude-packages', ','.join(self.exclude_packages)])

        if self.repositories:
            spark_cmd.extend(['--repositories', ','.join(self.repositories)])

        if self.property_file:
            spark_cmd.extend(['--properties-file', self.property_file])

        if self.klass:
            spark_cmd.extend(['--class', self.klass])

        if self.reqs_paths:
            ps = [r.strip() for r in chain(*[[str(p.resolve())
                                              for p in chain(Path(rp).rglob('*.egg'),
                                                             Path(rp).rglob('*.whl'),
                                                             Path(rp).rglob('*.zip'))
                                              if p.is_file()] for rp in self.reqs_paths]) if r.strip()]

            if len(ps):
                spark_cmd.extend(['--py-files', ','.join(ps)])

        return spark_cmd


class SparkSubmitCommand(BaseSparkCommand):

    def build_command(self, *, job_args: Iterable[str], **kwargs):
        kwargs.setdefault('executable', self.spark_executable)
        spark_cmd = super(SparkSubmitCommand, self).build_command(**kwargs)

        spark_cmd.extend(job_args)

        return spark_cmd

    def run(self, job_args: Iterable[str]):
        self.logger.info('Executing Spark job...')
        spark_command = self.build_command(job_args=job_args)

        env = os.environ.copy()

        env['PYSPARK_PYTHON'] = sys.executable
        env['PYSPARK_DRIVER_PYTHON'] = sys.executable

        env.update(self.env or {})

        self.logger.info(' '.join(spark_command))

        process = ProcessManager(spark_command, pass_through=True, env=env)
        process.start_process()
        process.wait()

        if process.returncode != 0:
            raise RuntimeError(f'Spark job failed with error: {process.returncode}')


class SparkInteractiveCommand(BaseSparkCommand):

    def __init__(self,
                 cmd_config,
                 *args,
                 pyspark_executable: str = None,
                 python_interactive_driver: str = None,
                 **kwargs):
        super(SparkInteractiveCommand, self).__init__(cmd_config, *args, **kwargs)

        try:
            cmd_config = cmd_config['interactive']
        except (KeyError, TypeError):
            cmd_config = ConfigParser(default_sections=('interactive',))
            cmd_config = cmd_config['interactive']

        self.pyspark_executable = pyspark_executable or cmd_config.get('pyspark-executable', fallback='pyspark')
        self.python_interactive_driver = python_interactive_driver or cmd_config.get('python-interactive-driver',
                                                                                     fallback=sys.executable)

        # Force client deploy mode
        self.deploy_mode = 'client'

    def build_command(self, **kwargs):
        kwargs.setdefault('executable', self.pyspark_executable)
        return super(SparkInteractiveCommand, self).build_command(**kwargs)

    def run(self):

        self.logger.info('Executing Spark interactive...')

        spark_command = self.build_command()

        env = os.environ.copy()

        env['PYSPARK_PYTHON'] = sys.executable
        env['PYSPARK_DRIVER_PYTHON'] = self.python_interactive_driver

        env.update(self.env)

        self.logger.info(' '.join(spark_command))

        process = ProcessManager(spark_command, pass_through=True, env=env)
        process.start_process()
        process.wait()

        if process.returncode != 0:
            raise RuntimeError(f'Interactive Spark failed with error: {process.returncode}')
