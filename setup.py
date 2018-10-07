from setuptools import setup, find_packages

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='poremongo',
    url='https://github.com/esteinig/poremongo',
    author='Eike J. Steinig',
    author_email='eikejoachim.steinig@my.jcu.edu.au',
    packages=find_packages(),
    install_requires=['numpy', 'tqdm', 'colorama', 'pymongo', 'mongoengine', 'ont_fast5_api',
                      'pandas', 'paramiko', 'scp', 'scikit-image', 'scipy', 'watchdog', 'apscheduler'],
    version='0.3',
    license='MIT',
    description='Nanopore sequence read and raw signal data management with MongoDB'
)