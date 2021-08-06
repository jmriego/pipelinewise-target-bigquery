#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-bigquery",
      version="1.1.1",
      description="Singer.io target for loading data to BigQuery - PipelineWise compatible",
      long_description=long_description,
      long_description_content_type='text/markdown',
      author="jmriego",
      url='https://github.com/transferwise/pipelinewise-target-bigquery',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=["target_bigquery"],
      install_requires=[
          'pipelinewise-singer-python==1.*',
          'google-cloud-bigquery==2.20.0',
          'joblib>=0.14,<1.1',
          'inflection==0.3.1',
          'fastavro==0.22.8'
      ],
      extras_require={
          "test": [
              'pytest==6.2.1',
              'pylint==2.6.0',
              'pytest-cov==2.10.1',
          ]
      },
      entry_points="""
          [console_scripts]
          target-bigquery=target_bigquery:main
      """,
      packages=["target_bigquery"],
      package_data={},
      include_package_data=True,
)
