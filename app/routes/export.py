import uuid
import asyncio
import os
import zlib
import asyncpg
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request, Response, status
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, DBAPIError
from database import AsyncSessionLocal, DATABASE_URL

router = APIRouter()
EXPORT_DIR = os.getenv("EXPORT_DIR", "/app/exports")
EXPORT_ORDERED = os.getenv("EXPORT_ORDERED", "false").lower() == "true"
EXPORT_COUNT_MODE = os.getenv("EXPORT_COUNT_MODE", "estimated").lower()
EXPORT_RUNNING_TASKS: dict[str, asyncio.Task] = {}
ALLOWED_COLUMNS = [
    "id",
    "name",
    "email",
    "signup_date",
    "country_code",
    "subscription_tier",
    "lifetime_value",
]


def _export_file_path(export_id: str) -> str:
    return os.path.join(EXPORT_DIR, f"{export_id}.csv")


def _asyncpg_dsn() -> str:
    return DATABASE_URL.replace("+asyncpg", "")


def _parse_columns(columns: str | None) -> list[str]:
    if not columns:
        return ALLOWED_COLUMNS.copy()

    parsed = [column.strip() for column in columns.split(",") if column.strip()]
    if not parsed:
        raise HTTPException(status_code=400, detail="columns must not be empty")

    invalid = [column for column in parsed if column not in ALLOWED_COLUMNS]
    if invalid:
        raise HTTPException(status_code=400, detail=f"invalid columns: {', '.join(invalid)}")

    return parsed


def _validate_delimiter(delimiter: str) -> str:
    if len(delimiter) != 1 or delimiter in {"\n", "\r"}:
        raise HTTPException(status_code=400, detail="delimiter must be a single visible character")
    return delimiter


def _gzip_file_stream(file_path: str, chunk_size: int = 1024 * 64):
    compressor = zlib.compressobj(wbits=31)
    with open(file_path, "rb") as file_handle:
        while True:
            chunk = file_handle.read(chunk_size)
            if not chunk:
                break
            compressed = compressor.compress(chunk)
            if compressed:
                yield compressed
    tail = compressor.flush()
    if tail:
        yield tail


async def _is_cancelled(session, export_id: str) -> bool:
    status_result = await session.execute(
        text("SELECT status FROM export_jobs WHERE id = :id"),
        {"id": export_id},
    )
    current_status = status_result.scalar_one_or_none()
    return current_status == "cancelled"


def _build_filters(country_code: str | None, min_ltv: float | None):
    sql_clauses: list[str] = []
    pg_clauses: list[str] = []
    sql_params: dict[str, object] = {}
    pg_args: list[object] = []

    if country_code:
        normalized_country = country_code.strip().upper()
        if len(normalized_country) != 2:
            raise HTTPException(status_code=400, detail="country_code must be a 2-letter code")
        sql_clauses.append("country_code = :country_code")
        sql_params["country_code"] = normalized_country
        pg_clauses.append(f"country_code = ${len(pg_args) + 1}")
        pg_args.append(normalized_country)

    if min_ltv is not None:
        sql_clauses.append("lifetime_value >= :min_ltv")
        sql_params["min_ltv"] = min_ltv
        pg_clauses.append(f"lifetime_value >= ${len(pg_args) + 1}")
        pg_args.append(min_ltv)

    where_sql = f" WHERE {' AND '.join(sql_clauses)}" if sql_clauses else ""
    where_pg = f" WHERE {' AND '.join(pg_clauses)}" if pg_clauses else ""
    return where_sql, where_pg, sql_params, pg_args


async def _resolve_total_rows(session, where_sql: str, sql_params: dict[str, object]) -> int:
    if EXPORT_COUNT_MODE == "none":
        return 0

    if EXPORT_COUNT_MODE == "estimated" and not where_sql:
        estimate_result = await session.execute(
            text(
                """
                SELECT COALESCE(reltuples, 0)::BIGINT
                FROM pg_class
                WHERE oid = 'users'::regclass
                """
            )
        )
        return int(estimate_result.scalar_one() or 0)

    exact_result = await session.execute(
        text(f"SELECT COUNT(*) FROM users{where_sql}"),
        sql_params,
    )
    return int(exact_result.scalar_one())

@router.post("/exports/csv", status_code=202)
async def initiate_export(
    background_tasks: BackgroundTasks,
    columns: str | None = Query(default=None),
    delimiter: str = Query(default=","),
    country_code: str | None = Query(default=None),
    min_ltv: float | None = Query(default=None),
):
    export_id = str(uuid.uuid4())
    selected_columns = _parse_columns(columns)
    csv_delimiter = _validate_delimiter(delimiter)

    max_attempts = 6
    for attempt in range(1, max_attempts + 1):
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(
                    text("""
                        INSERT INTO export_jobs (id, status)
                        VALUES (:id, 'pending')
                    """),
                    {"id": export_id}
                )
                await session.commit()
            break
        except (OperationalError, DBAPIError, OSError):
            if attempt == max_attempts:
                raise HTTPException(
                    status_code=503,
                    detail="database temporarily unavailable, retry shortly",
                )
            await asyncio.sleep(min(0.25 * (2 ** (attempt - 1)), 2.0))

    background_tasks.add_task(
        process_export,
        export_id,
        selected_columns,
        csv_delimiter,
        country_code,
        min_ltv,
    )

    return {
        "exportId": export_id,
        "status": "pending"
    }


@router.get("/exports/{export_id}")
async def get_export_status(export_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(
                """
                SELECT id, status, total_rows, processed_rows, error, created_at, completed_at
                FROM export_jobs
                WHERE id = :id
                """
            ),
            {"id": export_id},
        )
        row = result.mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Export job not found")

    response = {
        "exportId": str(row["id"]),
        "status": row["status"],
        "totalRows": row["total_rows"] or 0,
        "processedRows": row["processed_rows"] or 0,
        "error": row["error"],
        "createdAt": row["created_at"].isoformat() if row["created_at"] else None,
        "completedAt": row["completed_at"].isoformat() if row["completed_at"] else None,
    }
    if response["totalRows"] > 0:
        response["percentage"] = round((response["processedRows"] / response["totalRows"]) * 100, 2)
    else:
        response["percentage"] = 0.0

    if row["status"] == "completed":
        response["filePath"] = _export_file_path(export_id)
        response["downloadUrl"] = f"/exports/{export_id}/download"

    return response


@router.get("/exports/{export_id}/status")
async def get_export_status_alias(export_id: str):
    return await get_export_status(export_id)


@router.delete("/exports/{export_id}", status_code=status.HTTP_204_NO_CONTENT)
async def cancel_export(export_id: str):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            text(
                """
                UPDATE export_jobs
                SET status = 'cancelled',
                    completed_at = NOW(),
                    error = NULL
                WHERE id = :id AND status IN ('pending', 'processing')
                """
            ),
            {"id": export_id},
        )
        await session.commit()

        if result.rowcount == 0:
            exists_result = await session.execute(
                text("SELECT 1 FROM export_jobs WHERE id = :id"),
                {"id": export_id},
            )
            if exists_result.scalar_one_or_none() is None:
                raise HTTPException(status_code=404, detail="Export job not found")

    task = EXPORT_RUNNING_TASKS.get(export_id)
    if task and not task.done():
        task.cancel()

    file_path = _export_file_path(export_id)
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
        except OSError:
            pass

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.head("/exports/{export_id}/download")
async def download_export_head(export_id: str, request: Request):
    file_path = _export_file_path(export_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Export file not found")

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": f'attachment; filename="{export_id}.csv"',
    }
    if "gzip" in request.headers.get("accept-encoding", "").lower():
        headers["Content-Encoding"] = "gzip"
        headers["Vary"] = "Accept-Encoding"
    else:
        headers["Content-Length"] = str(os.path.getsize(file_path))

    return Response(status_code=200, media_type="text/csv", headers=headers)


@router.get("/exports/{export_id}/download")
async def download_export(export_id: str, request: Request):
    file_path = _export_file_path(export_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Export file not found")

    range_header = request.headers.get("range")
    accept_encoding = request.headers.get("accept-encoding", "").lower()

    if range_header:
        return FileResponse(
            path=file_path,
            filename=f"{export_id}.csv",
            media_type="text/csv",
            headers={"Accept-Ranges": "bytes"},
        )

    if "gzip" in accept_encoding:
        headers = {
            "Content-Disposition": f'attachment; filename="{export_id}.csv.gz"',
            "Content-Encoding": "gzip",
            "Vary": "Accept-Encoding",
        }
        return StreamingResponse(
            _gzip_file_stream(file_path),
            media_type="text/csv",
            headers=headers,
        )

    return FileResponse(
        path=file_path,
        filename=f"{export_id}.csv",
        media_type="text/csv",
        headers={"Accept-Ranges": "bytes"},
    )

async def process_export(
    export_id: str,
    selected_columns: list[str],
    csv_delimiter: str,
    country_code: str | None,
    min_ltv: float | None,
):
    current_task = asyncio.current_task()
    if current_task:
        EXPORT_RUNNING_TASKS[export_id] = current_task

    async with AsyncSessionLocal() as session:
        try:
            await session.execute(
                text("""
                    UPDATE export_jobs
                    SET status = 'processing'
                    WHERE id = :id AND status = 'pending'
                """),
                {"id": export_id}
            )
            await session.commit()

            if await _is_cancelled(session, export_id):
                return

            os.makedirs(EXPORT_DIR, exist_ok=True)
            file_path = _export_file_path(export_id)
            where_sql, where_pg, sql_params, pg_args = _build_filters(country_code, min_ltv)

            total_rows = await _resolve_total_rows(session, where_sql, sql_params)
            await session.execute(
                text("""
                    UPDATE export_jobs
                    SET total_rows = :total_rows
                    WHERE id = :id
                """),
                {"id": export_id, "total_rows": total_rows},
            )
            await session.commit()

            select_clause = ", ".join(selected_columns)
            order_by_clause = " ORDER BY id" if EXPORT_ORDERED else ""
            copy_query = f"SELECT {select_clause} FROM users{where_pg}{order_by_clause}"

            conn = await asyncpg.connect(_asyncpg_dsn())
            try:
                await conn.copy_from_query(
                    copy_query,
                    *pg_args,
                    output=file_path,
                    format="csv",
                    header=True,
                    delimiter=csv_delimiter,
                )
            finally:
                await conn.close()

            if total_rows > 0:
                processed_rows = total_rows
            else:
                exact_result = await session.execute(
                    text(f"SELECT COUNT(*) FROM users{where_sql}"),
                    sql_params,
                )
                processed_rows = int(exact_result.scalar_one())

            await session.execute(
                text("""
                    UPDATE export_jobs
                    SET processed_rows = :processed_rows,
                        total_rows = CASE WHEN total_rows = 0 THEN :processed_rows ELSE total_rows END
                    WHERE id = :id
                """),
                {"id": export_id, "processed_rows": processed_rows},
            )
            await session.commit()

            if not await _is_cancelled(session, export_id):
                await session.execute(
                    text("""
                        UPDATE export_jobs
                        SET status = 'completed',
                            completed_at = NOW()
                        WHERE id = :id
                    """),
                    {"id": export_id}
                )
                await session.commit()
        except asyncio.CancelledError:
            await session.execute(
                text("""
                    UPDATE export_jobs
                    SET status = 'cancelled',
                        completed_at = NOW(),
                        error = NULL
                    WHERE id = :id
                """),
                {"id": export_id},
            )
            await session.commit()
            file_path = _export_file_path(export_id)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except OSError:
                    pass
            raise
        except Exception as exc:
            await session.execute(
                text("""
                    UPDATE export_jobs
                    SET status = 'failed',
                        error = :error
                    WHERE id = :id AND status <> 'cancelled'
                """),
                {"id": export_id, "error": str(exc)},
            )
            await session.commit()
            raise
        finally:
            EXPORT_RUNNING_TASKS.pop(export_id, None)