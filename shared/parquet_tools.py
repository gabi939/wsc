import pandas as pd
from venv import logger
import pyarrow as pa
import pyarrow.parquet as pq
import io


type_map = {
    "string": pa.string(),
    "int": pa.int64(),
    "float": pa.float64(),
    "bool": pa.bool_(),
}


def _build_schema(schema: dict) -> pa.Schema:
    fields = [pa.field(name, type_map[dtype]) for name, dtype in schema.items()]
    return pa.schema(fields)


def create_parquet(data: list[dict], schema, compression="snappy") -> bytes:
    if not data:
        logger.warning("No data provided to create Parquet file")
        return

    try:
        df = pd.DataFrame(data)
        schema = _build_schema(schema)
        table = pa.Table.from_pandas(df, schema=schema)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression=compression)

        parquet_bytes = buf.getvalue()

        return parquet_bytes

    except Exception as e:
        logger.error(f"Error creating Parquet file: {e}", exc_info=True)
        raise


def read_parquet(parquet) -> list[dict]:
    try:
        buffer = io.BytesIO(parquet)
        table = pq.read_table(buffer)
        df = table.to_pandas()
        records = df.to_dict(orient="records")
        return records

    except Exception as e:
        logger.error(
            f"Failed to read Parquet bytes: {str(e)}",
            extra={"component": "parquet", "tags": {"error": type(e).__name__}},
        )
