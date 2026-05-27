from pathlib import Path

import setuptools

VERSION = "1.0.15"

requirements = []
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='py-hydra',
    author='John Rusnak',
    author_email='jrusnak69@gmail.com',
    version=VERSION,
    description="distributed, rpc",
    package_dir={'': 'src'},
    package_data={'': []},
    include_package_data=True,
    packages=setuptools.find_namespace_packages('src'),
    entry_points={
    },
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=["Development Status :: 4 - Beta",
                 'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)'],
    license='LGPL-3.0-only',
    keywords='distributed features and utilities for multi-host applications',
    url='https://github.com/nak/hydra',
    download_url="https://github.com/nak/hydra/dist/%s" % VERSION,
    install_requires=requirements
)
