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
       "pyspark>=3.0.0",
       "tqdm>=4.50.0",
    ],
)
