import signal
import sys
from io import StringIO, TextIOWrapper
from subprocess import PIPE, Popen
from typing import Dict, List, Optional


class ProcessManager:

    def __init__(self,
                 params: List[str],
                 env: Dict = None,
                 pass_through=False):

        self._current_process: Optional[Popen] = None

        self._params = params
        self._env = env

        if pass_through:
            self._stdin_stream = sys.stdin
            self._stdout_stream = sys.stdout
            self._stderr_stream = sys.stderr
        else:
            self._stdin_stream = sys.stdin
            self._stdout_stream = StringIO()
            self._stderr_stream = StringIO()

        self._original_sigint_handler = signal.getsignal(signal.SIGINT)
        self._original_sigterm_handler = signal.getsignal(signal.SIGTERM)

    def send_signal(self, sig, _=None):
        if self._current_process:
            self._current_process.send_signal(sig)

    def start_process(self):

        stdout = self._stdout_stream
        stderr = self._stderr_stream
        if isinstance(self._stdout_stream, StringIO):
            stdout = PIPE
            stderr = PIPE

        self._current_process = Popen(self._params,
                                      stdout=stdout,
                                      stderr=stderr,
                                      stdin=self._stdin_stream,
                                      env=self._env)

        signal.signal(signal.SIGTERM, self.send_signal)
        signal.signal(signal.SIGINT, self.send_signal)

        if stdout == PIPE:
            from concurrent.futures.thread import ThreadPoolExecutor
            pool = ThreadPoolExecutor(max_workers=2)

            def copy_stream(src, dst, buffer=1):
                while True:
                    data = src.read(buffer)
                    if len(data) == 0:
                        break
                    dst.write(data)

            pool.submit(copy_stream,
                        TextIOWrapper(self._current_process.stdout),
                        self._stdout_stream)
            pool.submit(copy_stream,
                        TextIOWrapper(self._current_process.stderr),
                        self._stderr_stream)

    def __enter__(self):
        self.start_process()
        self._current_process.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._current_process.__exit__(exc_type, exc_val, exc_tb)
        self.wait()

    @property
    def returncode(self) -> int:
        return self._current_process.returncode

    def wait(self, timeout=None) -> int:
        result = self._current_process.wait(timeout=timeout)

        if self._current_process.returncode != 0:
            if isinstance(self._stdout_stream, StringIO):
                self._stdout_stream.seek(0)
                sys.stdout.write(self._stdout_stream.read())
            if isinstance(self._stderr_stream, StringIO):
                self._stderr_stream.seek(0)
                sys.stderr.write(self._stderr_stream.read())

        signal.signal(signal.SIGTERM, self._original_sigterm_handler)
        signal.signal(signal.SIGINT, self._original_sigint_handler)
        return result
