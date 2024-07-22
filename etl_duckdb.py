import duckdb
import time

def create_duckdb():
    result = duckdb.sql("""
        SELECT cidade,
            MIN(temperatura) AS min_temperature,
            CAST(AVG(temperatura) AS DECIMAL(3,1)) AS mean_temperature,
            MAX(temperatura) AS max_temperature
        FROM read_csv("data/measurements.txt", AUTO_DETECT=FALSE, sep=';', columns={'cidade':VARCHAR, 'temperatura': 'DECIMAL(3,1)'})
        GROUP BY cidade
        ORDER BY cidade
    """)
    result.show()
    result.write_parquet("data/measurements_summary.parquet")

if __name__ == "__main__":
    import time
    start_time = time.time()
    create_duckdb()
    took = time.time() - start_time
    print(f"Duckdb Took: {took:.2f} sec")