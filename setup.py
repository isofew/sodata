from setuptools import setup

setup(
    name='sodata',
    version='0.0.1',
    author='Zhixing Wang',
    author_email='isofew@gmail.com',
    packages=['sodata', 'sodata.scripts', 'sodata.studies'],
    scripts=[],
    url='http://github.com/isofew/sodata',
    license='LICENSE.txt',
    description='',
    long_description=open('README.md').read(),
    install_requires=[
        "pyspark==3.0.*",
        "tqdm==4.50.*",
        "numpy==1.18.*",
        "pandas==0.25.*",
        "matplotlib==3.3.*",
        "seaborn==0.11.*",
        "requests==2.24.*",
        "libarchive==0.4.*",
        "torch==1.7.*",
    ],
)
