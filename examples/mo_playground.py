import marimo

__generated_with = "0.19.2"
app = marimo.App()


@app.cell
def _():
    from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

    return (PyarrowDatasetIO,)


@app.cell
def _(PyarrowDatasetIO):
    PyarrowDatasetIO()
    return


if __name__ == "__main__":
    app.run()
