import ast
from pathlib import Path

from setuptools import find_packages, setup

PACKAGE_NAME = 'sparpy'

path = Path(Path(__file__).parent, PACKAGE_NAME, '__init__.py')

with open(path, 'r') as file:
    t = compile(file.read(), str(path), 'exec', ast.PyCF_ONLY_AST)
    for node in (n for n in t.body if isinstance(n, ast.Assign)):
        if len(node.targets) != 1:
            continue

        name = node.targets[0]
        if not isinstance(name, ast.Name) or \
                name.id not in ('__version__', '__version_info__', 'VERSION'):
            continue

        v = node.value
        if isinstance(v, ast.Str):
            version = v.s
            break
        if isinstance(v, ast.Tuple):
            r = []
            for e in v.elts:
                if isinstance(e, ast.Str):
                    r.append(e.s)
                elif isinstance(e, ast.Num):
                    r.append(str(e.n))
            version = '.'.join(r)
            break

with Path(Path(__file__).parent, 'requirements.txt').open() as f:
    requirements = [l.strip()
                    for l in f.readlines()
                    if len(l.strip()) and not (l.strip().startswith('-') or l.strip().startswith('#'))]

# Get the long description from the README file
with Path(Path(__file__).parent, 'README.rst').open(encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='sparpy',

    version=version,

    description='A spark entry point for python',
    long_description=long_description,

    url='https://github.com/alfred82santa/sparpy',

    author='Alfred Santacatalina',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],

    packages=find_packages(exclude=['tests'], include=['sparpy*']),

    install_requires=requirements,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sparpy=sparpy.cli:run_sparpy',
            'sparpy-runner=sparpy.cli:run_sparpy_runner',
            'sparpy-submit=sparpy.cli:run_sparpy_submit',
        ]
    }
)
