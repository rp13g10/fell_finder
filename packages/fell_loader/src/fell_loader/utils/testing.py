"""Utility functions which facilitate test execution in other parts of
this package"""

import daft
from tabulate import tabulate

# TODO: Tidy this up and test it


def assert_frame_equal(
    result: daft.DataFrame,
    target: daft.DataFrame,
    order_by: str | list[str] | None = None,
) -> None:
    # Apply row sorting
    if order_by:
        if type(order_by) is str:
            order_by = [order_by]
        for sort_col in order_by:
            assert sort_col in result.column_names, (
                f"Provided sort column {sort_col} is not present in result."
                f"Available columns are: {result.column_names}"
            )
            assert sort_col in target.column_names, (
                f"Provided sort column {sort_col} is not present in target."
                f"Available columns are: {target.column_names}"
            )
        result = result.sort(by=order_by)  # type: ignore
        target = target.sort(by=order_by)  # type: ignore

    # Check headers
    result_cols = sorted(result.column_names)
    target_cols = sorted(target.column_names)
    match = [x == y for x, y in zip(result_cols, target_cols, strict=False)]
    header_tbl = tabulate(
        {
            "result_cols": result_cols,
            "target_cols": target_cols,
            "match": match,
        },
        headers="keys",
    )
    header_msg = "Table headers do not match:\n" + header_tbl
    assert result_cols == target_cols, header_msg

    # Check schema
    result = result.select(*result_cols)
    target = target.select(*target_cols)
    result_schema = result.schema()
    target_schema = target.schema()
    schema_msg = (
        "Table schemas do not match\n\nresult_schema:\n"
        + repr(result_schema)
        + "\n\ntarget_schema:\n"
        + repr(target_schema)
    )
    assert result_schema == target_schema, schema_msg

    # Check contents
    result_repr = tabulate(result.to_pydict(), headers="keys")
    target_repr = tabulate(target.to_pydict(), headers="keys")
    content_msg = (
        f"Table content does not match:\n\nresult:\n{result_repr}"
        f"\n\ntarget:\n{target_repr}"
    )
    assert result.to_pydict() == target.to_pydict(), content_msg
