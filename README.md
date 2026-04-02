# submit_to_images

High-throughput PDF-to-image rendering with Rust + PyO3.

Core goals:

- Keep the hot path in Rust.
- Render multi-page PDFs concurrently.
- Support both sync and `asyncio` APIs from Python.
- Prefer throughput over small output size.

Current implementation:

- `PdfRenderQueue` keeps the queue and worker threads in Rust.
- Large PDFs are rendered concurrently across pages.
- Workers reuse the opened PDF document and pull pages dynamically.
- Rust returns a native `RenderFuture` object that supports both `await future` and `future.result()`.
- `AsyncPdfRenderQueue` is now a thin Python wrapper over the native future API.
- `jpg/jpeg` output is supported with `quality=1..100`.

## Install

```bash
python -m pip install maturin
maturin develop --release
```

Windows is pinned to `MUPDF_MSVC_PLATFORM_TOOLSET = "v143"` through `.cargo/config.toml`.
The first build is slow because MuPDF is built from source.

## GitHub Actions Linux Build

The repository now includes a Linux build workflow at
[`/.github/workflows/linux-dist.yml`](./.github/workflows/linux-dist.yml).

- Build container: `m.daocloud.io/docker.io/library/python:3.11-slim`
- Output: wheel + sdist uploaded as the `linux-dist` artifact
- Release mode: tags matching `v*` also publish the files to the GitHub Release
- Validation: the workflow installs the built wheel and runs a smoke test against the vendored `dummy.pdf`

Important:

- This image produces a regular `linux_x86_64` wheel bound to the container's glibc baseline.
- If you need a broadly portable PyPI Linux wheel, switch the workflow to a `manylinux` image instead of `python:3.11-slim`.

## Sync Example

```python
from submit_to_images import PdfRenderQueue, RenderJob

queue = PdfRenderQueue(workers=16, capacity=4096)

job_id = queue.submit(
    RenderJob(
        pdf_path=r"C:\data\input.pdf",
        output_dir=r"C:\data\output_jpg",
        render_dpi=200,
        format="jpg",
        quality=85,
        batch_pages=0,
        show_extras=True,
    )
)

result = None
while result is None:
    result = queue.recv(timeout_ms=100)

print(job_id, result.ok, result.rendered_pages, result.outputs[:3], result.errors)
queue.close()
```

## Async Example

```python
import asyncio

from submit_to_images import PdfRenderQueue, RenderJob


async def main() -> None:
    queue = PdfRenderQueue(workers=16, capacity=4096)
    future = queue.submit_future(
        RenderJob(
            pdf_path=r"C:\data\book.pdf",
            output_dir=r"C:\data\book_jpg",
            render_dpi=200,
            format="jpg",
            quality=82,
            batch_pages=0,
        )
    )

    result = await future
    print(result.ok, result.rendered_pages, result.outputs[:3])
    queue.close()


asyncio.run(main())
```

## Native Future Example

```python
from submit_to_images import PdfRenderQueue, RenderJob

queue = PdfRenderQueue(workers=16, capacity=4096)

future = queue.submit_future(
    RenderJob(
        pdf_path=r"C:\data\book.pdf",
        output_dir=r"C:\data\book_jpg",
        render_dpi=200,
        format="jpg",
        quality=82,
        batch_pages=0,
    )
)

result = future.result()
print(future.job_id, result.ok, result.rendered_pages)
queue.close()
```

## Async Batch Example

```python
import asyncio

from submit_to_images import AsyncPdfRenderQueue, RenderJob


async def main() -> None:
    jobs = [
        RenderJob(
            pdf_path=rf"C:\data\doc_{idx}.pdf",
            output_dir=rf"C:\data\out\doc_{idx}",
            render_dpi=180,
            format="jpg",
            quality=80,
            batch_pages=0,
        )
        for idx in range(8)
    ]

    async with AsyncPdfRenderQueue(workers=16, capacity=4096) as queue:
        handles = [queue.submit_nowait(job) for job in jobs]
        results = await asyncio.gather(*(handle.result() for handle in handles))

    for result in results:
        print(result.job_id, result.ok, result.rendered_pages)


asyncio.run(main())
```

## API Notes

- `RenderJob.page_indices` is 0-based.
- `RenderJob.render_dpi` is the preferred render resolution parameter.
- `RenderJob.dpi` is still accepted as a compatibility alias.
- `RenderJob.batch_pages = 0` means automatic multi-page concurrent scheduling.
- `RenderJob.quality` is used for `jpg/jpeg` output.
- `RenderFuture` can be awaited directly or consumed with `future.result()`.
- `AsyncPdfRenderQueue.aclose()` closes the queue asynchronously.
- Supported formats: `pnm`, `png`, `pam`, `psd`, `ps`, `jpg`.
- `alpha=True` should only be used with `png` or `pam`.

## Performance Notes

- `pnm` is still the fastest output format.
- `jpg` is usually slower than `pnm` because of compression, but much smaller on disk.
- Worker threads dynamically pull pages from the same PDF to improve core utilization.
- The native future path removes the Python-side result pump thread.

## License Note

`mupdf` / `mupdf-sys` are AGPL licensed.
If that is not acceptable for your distribution model, you need a different backend or a commercial license path.
