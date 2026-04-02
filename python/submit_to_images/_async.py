from __future__ import annotations

import asyncio
from typing import Iterable

from .submit_to_images_native import PdfRenderQueue, RenderFuture, RenderJob

AsyncRenderHandle = RenderFuture


class AsyncPdfRenderQueue:
    def __init__(self, workers: int | None = None, capacity: int = 2048) -> None:
        self._native = PdfRenderQueue(workers=workers, capacity=capacity)

    def submit_nowait(self, job: RenderJob) -> RenderFuture:
        return self._native.submit_future(job)

    async def submit(self, job: RenderJob) -> RenderFuture:
        return self.submit_nowait(job)

    def close(self) -> None:
        self._native.close()

    async def aclose(self) -> None:
        self._native.close()

    def stats(self):
        return self._native.stats()

    async def __aenter__(self) -> "AsyncPdfRenderQueue":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    def __enter__(self) -> "AsyncPdfRenderQueue":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


async def render_async(
    job: RenderJob,
    *,
    workers: int | None = None,
    capacity: int = 2048,
):
    async with AsyncPdfRenderQueue(workers=workers, capacity=capacity) as queue:
        future = queue.submit_nowait(job)
        return await future


async def render_many_async(
    jobs: Iterable[RenderJob],
    *,
    workers: int | None = None,
    capacity: int = 2048,
) -> list:
    async with AsyncPdfRenderQueue(workers=workers, capacity=capacity) as queue:
        futures = [queue.submit_nowait(job) for job in jobs]
        return await asyncio.gather(*futures)
