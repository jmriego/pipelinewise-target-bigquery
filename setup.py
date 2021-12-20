#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
    long_description = f.read()

setup(name="pipelinewise-target-bigquery",
      version="1.2.0",
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
          'joblib==1.1.0',
          'inflection==0.3.1',
          'fastavro==0.22.8'
      ],
      extras_require={
          "test": [
              'pytest==6.2.5',
              'pylint==2.12.2',
              'pytest-cov==3.0.0',
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
