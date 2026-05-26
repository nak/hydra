from pathlib import Path

import setuptools

VERSION = "1.0.0"

requirements = []
with open(Path(__file__).parent / 'requirements.txt', 'r') as in_stream:
    for line in in_stream:
        line = line.strip()
        if line:
            requirements.append(line)

setuptools.setup(
    name='pytest_mproc',
    author='John Rusnak',
    author_email='jrusnak69@gmail.com',
    version=VERSION,
    description="low-startup-overhead, scalable, distributed-testing pytest plugin",
    package_dir={'': 'src'},
    package_data={'': ['pure_requirements.txt', 'impure_requirements.txt']},
    include_package_data=True,
    packages=setuptools.find_packages('src'),
    entry_points={
    },
    classifiers=["Development Status :: 4 - Beta",
                 "License :: LGPL License"],
    license='LGPL v2',
    keywords='distributed features and utilities for multi-host applications',
    url='https://github.com/nak/hydra',
    download_url="https://github.com/nak/hydra/dist/%s" % VERSION,
    install_requires=requirements
)
