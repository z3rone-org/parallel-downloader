from setuptools import setup, find_packages

requirements = [
    "fsspec",
    "tqdm",
]

setup(
    name='parallel_downloader',
    author="Falk Boudewijn Schimweg",
    author_email='git@falk.schimweg.de',
    python_requires='>=3.10',
    use_scm_version=True,
    requirements=requirements,
    packages=find_packages(include=['parallel_downloader', 'parallel_downloader.*']),
    setup_requires=['setuptools_scm'],
)