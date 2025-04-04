# prefect-cpln

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-cpln/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-cpln?color=26272B&labelColor=090422"></a>
    <a href="https://pypistats.org/packages/prefect-cpln/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-cpln?color=26272B&labelColor=090422" /></a>
</p>

## Welcome!

`prefect-cpln` is a collection of Prefect tasks, flows, and blocks enabling orchestration, observation and management of Control Plane resources.

Jump to [examples](#example-usage).

## Resources

For more tips on how to use tasks and flows in an integration, check out [Use Integrations](https://docs.prefect.io/integrations/use-integrations)!

### Installation

Install `prefect-cpln` with `pip`:

```bash
 pip install prefect-cpln
```

Requires an installation of Python 3.8+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2. For more information about how to use Prefect, please refer to the [Prefect documentation](https://docs.prefect.io/).

Then, to register [blocks](https://docs.prefect.io/integrations/use-integrations#register-blocks-from-an-integration) on Prefect Cloud:

```bash
prefect block register -m prefect_cpln
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://docs.prefect.io/v3/develop/blocks#saving-blocks) or saved through the UI.

### Example Usage

#### Use `with_options` to customize options on any existing task or flow

```python
from prefect_cpln.flows import run_namespaced_job

customized_run_namespaced_job = run_namespaced_job.with_options(
    name="My flow running a Control Plane Job",
    retries=2,
    retry_delay_seconds=10,
) # this is now a new flow object that can be called
```

For more tips on how to use tasks and flows in an integration, check out [Use Integrations](https://docs.prefect.io/integrations/use-integrations)!

## Feedback

If you encounter any bugs while using `prefect-cpln`, feel free to open an issue in the [prefect-cpln](https://github.com/controlplane-com/prefect-cpln) repository.

If you have any questions or issues while using `prefect-cpln`, you can send an email to [support@controlplane.com](mailto:support@controlplane.com).

## Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-cpln`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:

```
pip install -e ".[dev]"
```

4. Make desired changes
5. Add tests
6. Install `pre-commit` to perform quality checks prior to commit:

```
 pre-commit install
```

8. `git commit`, `git push`, and create a pull request
