#!/usr/bin/env python

from setuptools import setup

with open('README.md') as f:
      long_description = f.read()

setup(name="pipelinewise-target-bigquery",
      version="1.0.0",
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
          'setuptools==40.3.0',
          'google-cloud-bigquery==1.23.0',
          'joblib==0.13.2',
          'inflection==0.3.1',
          'fastavro==0.22.8'
      ],
      extras_require={
          "test": [
              "nose==1.3.7",
              "mock==3.0.5",
              "pylint==2.4.2"
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
