# Kedro-DataSentinel

`kedro-datasentinel` is a [kedro-plugin](https://kedro.readthedocs.io/en/stable/extend_kedro/plugins.html) for seamless integration of Data Sentinel capabilities inside [kedro](https://kedro.readthedocs.io/en/stable/index.html) projects. It enforces Kedro principles to make data quality and validation as production-ready as possible. Its core functionalities are:

- **Data Validation**: `kedro-datasentinel` enhances data quality for machine learning and data engineering pipelines. With minimal configuration, you can validate your datasets during a kedro run, both online (during pipeline execution) and offline (post-execution).

- **Audit Logging**: Track and monitor your pipeline executions with detailed audit logs. This feature provides visibility into your data processing workflows, making it easier to debug issues and ensure compliance.

- **Notification System**: Get alerted when data quality issues arise. Configure notifications to be sent through various channels when validation checks fail.

## How do I install kedro-datasentinel?

You can install `kedro-datasentinel` with pip:

```bash
pip install kedro-datasentinel
```

For development installation:

```bash
pip install --upgrade git+https://github.com/SumzCol/kedro-datasentinel.git
```

We recommend using a package manager (like `conda`) to create a virtual environment and to read [kedro installation guide](https://docs.kedro.org/en/stable/get_started/minimal_kedro_project.html#step-1-install-kedro).

## Getting started

To use `kedro-datasentinel` in your Kedro project:

1. Install the package as described above
2. Create a `datasentinel.yml` configuration file in your project's `conf` directory
3. Configure your datasets with validation rules in your catalog
4. Run your Kedro pipeline as usual

## Features

### Data Validation

`kedro-datasentinel` provides a flexible framework for validating your data:

- **Online Validation**: Validate data during pipeline execution
- **Offline Validation**: Validate data after pipeline execution leveraging the command `datasentinel validate -d <dataset_name>`
- **Custom Checks**: Create your own validation checks
- **Integration with Data Sentinel**: Leverage all the capabilities of Data Sentinel

### Audit Logging

Track the execution of your Kedro pipelines with detailed audit logs:

- **Node Execution**: Log when nodes start, complete, or fail
- **Input/Output Tracking**: Record which datasets were used as inputs and outputs
- **Error Logging**: Capture exceptions and error messages
- **Multiple Storage Options**: Store audit logs in databases, files, or custom stores

### Notification System

Get alerted when data quality issues arise:

- **Email Notifications**: Send emails when validation checks fail
- **Custom Notifiers**: Create your own notification channels
- **Event-Based Triggers**: Configure which events trigger notifications

## Release and roadmap

The [release history](https://github.com/SumzCol/kedro-datasentinel/blob/master/CHANGELOG.md) centralizes package improvements across time.

## Disclaimer

This package is still in active development. We use [SemVer](https://semver.org/) principles to version our releases.

## Can I contribute?

We'd be happy to receive help to maintain and improve the package. Any PR will be considered (from typo in the docs to core features add-on). Please check the [contributing guidelines](https://github.com/SumzCol/kedro-datasentinel/blob/master/CONTRIBUTING.md).

## Main contributors

The following people actively maintain, enhance and discuss design to make this package as good as possible:

- [Sumz SAS Team](https://github.com/SumzCol)
