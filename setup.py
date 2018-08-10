from setuptools import setup, find_packages

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name='poremongo',
    url='https://github.com/esteinig/poremongo',
    author='Eike J. Steinig',
    author_email='eikejoachim.steinig@my.jcu.edu.au',
    packages=find_packages(),
    install_requires=['numpy', 'tqdm', 'colorama', 'pymongo', 'mongoengine'],
    version='0.1',
    license='MIT',
    description='Nanopore sequence read data management with MongoDB'
)