from setuptools import setup

setup(
    name="dbt_workflows_converter",
    install_requires="data_pipelines_cli",
    entry_points={"dbt_workflows_converter": ["dbt_workflows_converter = dbt_workflows_converter"]},
    py_modules=["dbt_workflows_converter"],
)
