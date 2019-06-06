from setuptools import setup, find_packages

setup(
    name="poremongo",
    url="https://github.com/esteinig/poremongo",
    author="Eike J. Steinig",
    author_email="eikejoachim.steinig@my.jcu.edu.au",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'numpy',
        'tqdm',
        'colorama',
        'pymongo',
        'mongoengine',
        'ont_fast5_api',
        'pandas',
        'paramiko',
        'scp',
        'scikit-image',
        'scipy',
        'watchdog',
        'apscheduler',
        'click'
    ],
    entry_points="""
        [console_scripts]
        poremongo=poremongo.terminal.client:terminal_client
    """,
    version="0.3",
    license="MIT",
    description="Nanopore sequence read and raw signal data management with MongoDB",
)
