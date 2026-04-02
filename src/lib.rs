use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded, unbounded};
use jpeg_encoder::{ColorType as JpegColorType, Encoder as JpegEncoder};
use mupdf::{Colorspace, Document, ImageFormat, Matrix};
use mupdf_sys::fz_matrix;
use parking_lot::Mutex;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

#[pyclass(module = "pdf_to_images", frozen, get_all)]
#[derive(Clone)]
struct RenderJob {
    pdf_path: String,
    output_dir: String,
    render_dpi: u32,
    format: String,
    quality: u8,
    page_indices: Option<Vec<u32>>,
    password: Option<String>,
    batch_pages: usize,
    prefix: Option<String>,
    alpha: bool,
    show_extras: bool,
}

#[pymethods]
impl RenderJob {
    #[new]
    #[pyo3(signature = (
        pdf_path,
        output_dir,
        render_dpi = None,
        dpi = None,
        format = "pnm".to_string(),
        quality = 90,
        page_indices = None,
        password = None,
        batch_pages = 0,
        prefix = None,
        alpha = false,
        show_extras = true
    ))]
    fn new(
        pdf_path: String,
        output_dir: String,
        render_dpi: Option<u32>,
        dpi: Option<u32>,
        format: String,
        quality: u8,
        page_indices: Option<Vec<u32>>,
        password: Option<String>,
        batch_pages: usize,
        prefix: Option<String>,
        alpha: bool,
        show_extras: bool,
    ) -> PyResult<Self> {
        let render_dpi = match (render_dpi, dpi) {
            (Some(render_dpi), Some(dpi)) if render_dpi != dpi => {
                return Err(PyValueError::new_err(
                    "render_dpi and dpi cannot have different values",
                ));
            }
            (Some(render_dpi), _) => render_dpi,
            (_, Some(dpi)) => dpi,
            (None, None) => 200,
        };

        if pdf_path.trim().is_empty() {
            return Err(PyValueError::new_err("pdf_path cannot be empty"));
        }
        if output_dir.trim().is_empty() {
            return Err(PyValueError::new_err("output_dir cannot be empty"));
        }
        if render_dpi == 0 {
            return Err(PyValueError::new_err("render_dpi must be greater than 0"));
        }
        if !(1..=100).contains(&quality) {
            return Err(PyValueError::new_err("quality must be between 1 and 100"));
        }
        if let Some(prefix) = &prefix {
            if prefix.contains('/') || prefix.contains('\\') || prefix.contains(':') {
                return Err(PyValueError::new_err(
                    "prefix must be a file prefix, not a path",
                ));
            }
        }

        Ok(Self {
            pdf_path,
            output_dir,
            render_dpi,
            format,
            quality,
            page_indices,
            password,
            batch_pages,
            prefix,
            alpha,
            show_extras,
        })
    }

    #[getter]
    fn dpi(&self) -> u32 {
        self.render_dpi
    }
}

#[pyclass(module = "pdf_to_images", frozen, get_all)]
#[derive(Clone)]
struct RenderResult {
    job_id: u64,
    ok: bool,
    requested_pages: usize,
    rendered_pages: usize,
    elapsed_ms: u64,
    outputs: Vec<String>,
    errors: Vec<String>,
}

#[pyclass(module = "pdf_to_images", frozen)]
struct RenderFuture {
    job_id: u64,
    future: Py<PyAny>,
}

#[pymethods]
impl RenderFuture {
    #[getter]
    fn job_id(&self) -> u64 {
        self.job_id
    }

    fn done(&self, py: Python<'_>) -> PyResult<bool> {
        self.future.bind(py).call_method0("done")?.extract()
    }

    fn cancelled(&self, py: Python<'_>) -> PyResult<bool> {
        self.future.bind(py).call_method0("cancelled")?.extract()
    }

    fn cancel(&self, py: Python<'_>) -> PyResult<bool> {
        self.future.bind(py).call_method0("cancel")?.extract()
    }

    #[pyo3(signature = (timeout = None))]
    fn result(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Py<PyAny>> {
        let value = match timeout {
            Some(timeout) => self.future.bind(py).call_method1("result", (timeout,))?,
            None => self.future.bind(py).call_method0("result")?,
        };
        Ok(value.unbind())
    }

    fn __await__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let asyncio = py.import("asyncio")?;
        let wrapped = asyncio.call_method1("wrap_future", (self.future.clone_ref(py),))?;
        Ok(wrapped.call_method0("__await__")?.unbind())
    }
}

#[pyclass(module = "pdf_to_images", frozen, get_all)]
struct QueueStats {
    workers: usize,
    capacity: usize,
    submitted_jobs: u64,
    completed_jobs: u64,
    pending_jobs: usize,
    queued_shards: usize,
    in_progress_shards: usize,
}

struct PythonFutureListener {
    future: Py<PyAny>,
}

#[pyclass(module = "pdf_to_images")]
struct PdfRenderQueue {
    inner: Arc<QueueInner>,
}

#[pymethods]
impl PdfRenderQueue {
    #[new]
    #[pyo3(signature = (workers = None, capacity = 2048))]
    fn new(workers: Option<usize>, capacity: usize) -> PyResult<Self> {
        Ok(Self {
            inner: QueueInner::new(workers, capacity)?,
        })
    }

    fn submit(&self, py: Python<'_>, job: PyRef<'_, RenderJob>) -> PyResult<u64> {
        let inner = Arc::clone(&self.inner);
        let job = (*job).clone();
        py.detach(move || inner.submit(job))
    }

    fn submit_future(&self, py: Python<'_>, job: PyRef<'_, RenderJob>) -> PyResult<RenderFuture> {
        let native_future = create_concurrent_future(py)?;
        let listener = PythonFutureListener {
            future: native_future.clone_ref(py),
        };
        let inner = Arc::clone(&self.inner);
        let job = (*job).clone();
        let job_id = py.detach(move || inner.submit_with_listener(job, Some(listener)))?;
        Ok(RenderFuture {
            job_id,
            future: native_future,
        })
    }

    #[pyo3(signature = (timeout_ms = None))]
    fn recv(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<Option<RenderResult>> {
        let inner = Arc::clone(&self.inner);
        let timeout = timeout_ms.map(Duration::from_millis);
        py.detach(move || inner.recv_one(timeout))
    }

    #[pyo3(signature = (max_items = 64, timeout_ms = None))]
    fn recv_many(
        &self,
        py: Python<'_>,
        max_items: usize,
        timeout_ms: Option<u64>,
    ) -> PyResult<Vec<RenderResult>> {
        let inner = Arc::clone(&self.inner);
        let timeout = timeout_ms.map(Duration::from_millis);
        py.detach(move || inner.recv_many(max_items, timeout))
    }

    fn stats(&self) -> QueueStats {
        self.inner.stats()
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.shutdown())
    }
}

impl Drop for PdfRenderQueue {
    fn drop(&mut self) {
        let _ = self.inner.shutdown();
    }
}

#[pyfunction]
fn recommended_workers() -> usize {
    default_worker_count(None)
}

#[pyfunction]
fn supported_formats() -> Vec<&'static str> {
    OutputFormat::supported()
}

#[pyfunction]
#[pyo3(signature = (job, workers = None, capacity = 2048))]
fn render(
    py: Python<'_>,
    job: PyRef<'_, RenderJob>,
    workers: Option<usize>,
    capacity: usize,
) -> PyResult<RenderResult> {
    let job = (*job).clone();
    py.detach(move || {
        let queue = PdfRenderQueue::new(workers, capacity)?;
        let _job_id = queue.inner.submit(job)?;
        let result = queue
            .inner
            .recv_one(None)?
            .ok_or_else(|| PyRuntimeError::new_err("queue stopped before producing a result"))?;
        queue.inner.shutdown()?;
        Ok(result)
    })
}

#[pymodule]
fn pdf_to_images_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RenderJob>()?;
    m.add_class::<RenderResult>()?;
    m.add_class::<RenderFuture>()?;
    m.add_class::<QueueStats>()?;
    m.add_class::<PdfRenderQueue>()?;
    m.add_function(wrap_pyfunction!(recommended_workers, m)?)?;
    m.add_function(wrap_pyfunction!(supported_formats, m)?)?;
    m.add_function(wrap_pyfunction!(render, m)?)?;
    Ok(())
}

struct QueueInner {
    work_tx: Sender<WorkItem>,
    ready_rx: Receiver<RenderResult>,
    jobs: Mutex<JobBook>,
    joins: Mutex<Option<Vec<JoinHandle<()>>>>,
    aggregator_join: Mutex<Option<JoinHandle<()>>>,
    next_job_id: AtomicU64,
    submitted_jobs: AtomicU64,
    completed_jobs: AtomicU64,
    queued_shards: AtomicUsize,
    in_progress_shards: AtomicUsize,
    worker_count: usize,
    capacity: usize,
    closed: AtomicBool,
}

impl QueueInner {
    fn new(workers: Option<usize>, capacity: usize) -> PyResult<Arc<Self>> {
        let worker_count = default_worker_count(workers);
        let capacity = capacity.max(worker_count * 2).max(1);
        let (work_tx, work_rx) = bounded::<WorkItem>(capacity);
        let (result_tx, result_rx) = bounded::<WorkerResult>(capacity.max(worker_count * 4));
        let (ready_tx, ready_rx) = unbounded::<RenderResult>();

        let inner = Arc::new(Self {
            work_tx,
            ready_rx,
            jobs: Mutex::new(JobBook::default()),
            joins: Mutex::new(Some(Vec::with_capacity(worker_count))),
            aggregator_join: Mutex::new(None),
            next_job_id: AtomicU64::new(1),
            submitted_jobs: AtomicU64::new(0),
            completed_jobs: AtomicU64::new(0),
            queued_shards: AtomicUsize::new(0),
            in_progress_shards: AtomicUsize::new(0),
            worker_count,
            capacity,
            closed: AtomicBool::new(false),
        });

        let mut handles = Vec::with_capacity(worker_count);
        for idx in 0..worker_count {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            let inner_ref = Arc::clone(&inner);
            let handle = thread::Builder::new()
                .name(format!("pdf-render-worker-{idx}"))
                .spawn(move || worker_loop(idx, work_rx, result_tx, inner_ref))
                .map_err(to_py_runtime_error)?;
            handles.push(handle);
        }
        *inner.joins.lock() = Some(handles);

        let aggregator_inner = Arc::clone(&inner);
        let aggregator = thread::Builder::new()
            .name("pdf-render-aggregator".to_string())
            .spawn(move || aggregator_loop(result_rx, ready_tx, aggregator_inner))
            .map_err(to_py_runtime_error)?;
        *inner.aggregator_join.lock() = Some(aggregator);

        Ok(inner)
    }

    fn submit(&self, job: RenderJob) -> PyResult<u64> {
        self.submit_with_listener(job, None)
    }

    fn submit_with_listener(
        &self,
        job: RenderJob,
        listener: Option<PythonFutureListener>,
    ) -> PyResult<u64> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(PyRuntimeError::new_err("queue is already closed"));
        }

        let prepared = PreparedJob::from_request(job, self.worker_count)?;
        let job_id = self.next_job_id.fetch_add(1, Ordering::SeqCst);
        let shard_count = prepared.token_count;

        if shard_count == 0 {
            return Err(PyRuntimeError::new_err("job contains no pages to render"));
        }

        {
            let mut jobs = self.jobs.lock();
            jobs.pending
                .insert(job_id, JobState::new(&prepared, shard_count, listener));
        }

        let spec = Arc::new(prepared.spec);
        let runtime = Arc::new(JobRuntime {
            job_id,
            spec,
            pages: Arc::new(prepared.requested_pages.clone()),
            next_page: AtomicUsize::new(0),
            pages_per_token: prepared.pages_per_token,
        });
        for _ in 0..shard_count {
            self.queued_shards.fetch_add(1, Ordering::SeqCst);
            let work = WorkItem::Render(WorkToken {
                runtime: Arc::clone(&runtime),
            });
            if let Err(err) = self.work_tx.send(work) {
                self.queued_shards.fetch_sub(1, Ordering::SeqCst);
                self.jobs.lock().pending.remove(&job_id);
                return Err(PyRuntimeError::new_err(format!(
                    "failed to enqueue render work: {err}"
                )));
            }
        }

        self.submitted_jobs.fetch_add(1, Ordering::SeqCst);
        Ok(job_id)
    }

    fn recv_one(&self, timeout: Option<Duration>) -> PyResult<Option<RenderResult>> {
        match timeout {
            Some(timeout) => match self.ready_rx.recv_timeout(timeout) {
                Ok(result) => Ok(Some(result)),
                Err(RecvTimeoutError::Timeout) => Ok(None),
                Err(RecvTimeoutError::Disconnected) => Ok(None),
            },
            None => Ok(self.ready_rx.recv().ok()),
        }
    }

    fn recv_many(
        &self,
        max_items: usize,
        timeout: Option<Duration>,
    ) -> PyResult<Vec<RenderResult>> {
        if max_items == 0 {
            return Ok(Vec::new());
        }

        let first = match timeout {
            Some(duration) => match self.ready_rx.recv_timeout(duration) {
                Ok(result) => Some(result),
                Err(RecvTimeoutError::Timeout) => None,
                Err(RecvTimeoutError::Disconnected) => None,
            },
            None => self.ready_rx.recv().ok(),
        };

        let Some(first) = first else {
            return Ok(Vec::new());
        };

        let mut ready = vec![first];
        while ready.len() < max_items {
            let Ok(result) = self.ready_rx.try_recv() else {
                break;
            };
            ready.push(result);
        }
        Ok(ready)
    }

    fn stats(&self) -> QueueStats {
        let pending_jobs = self.jobs.lock().pending.len();
        QueueStats {
            workers: self.worker_count,
            capacity: self.capacity,
            submitted_jobs: self.submitted_jobs.load(Ordering::SeqCst),
            completed_jobs: self.completed_jobs.load(Ordering::SeqCst),
            pending_jobs,
            queued_shards: self.queued_shards.load(Ordering::SeqCst),
            in_progress_shards: self.in_progress_shards.load(Ordering::SeqCst),
        }
    }

    fn shutdown(&self) -> PyResult<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        for _ in 0..self.worker_count {
            let _ = self.work_tx.send(WorkItem::Shutdown);
        }

        if let Some(handles) = self.joins.lock().take() {
            for handle in handles {
                handle
                    .join()
                    .map_err(|_| PyRuntimeError::new_err("worker thread panicked"))?;
            }
        }

        if let Some(handle) = self.aggregator_join.lock().take() {
            handle
                .join()
                .map_err(|_| PyRuntimeError::new_err("aggregator thread panicked"))?;
        }

        Ok(())
    }

    fn ingest_worker_result(
        &self,
        result: WorkerResult,
    ) -> Option<(RenderResult, Option<PythonFutureListener>)> {
        let mut jobs = self.jobs.lock();
        let Some(mut state) = jobs.pending.remove(&result.job_id) else {
            return None;
        };

        for page in result.outputs {
            if let Some(slot) = state.page_to_slot.get(&page.page_index).copied() {
                if state.outputs[slot].is_none() {
                    state.outputs[slot] = Some(page.path);
                    state.rendered_pages += 1;
                }
            }
        }

        for error in result.errors {
            if !state.errors.contains(&error) {
                state.errors.push(error);
            }
        }
        state.remaining_shards = state.remaining_shards.saturating_sub(1);

        if state.remaining_shards == 0 {
            for (slot, page_index) in state.requested_page_indices.iter().enumerate() {
                if state.outputs[slot].is_none() {
                    state
                        .errors
                        .push(format!("page {} did not produce an output", page_index + 1));
                }
            }

            let outputs = state.outputs.into_iter().flatten().collect::<Vec<_>>();
            let ok = state.errors.is_empty() && outputs.len() == state.requested_pages;
            let result = RenderResult {
                job_id: result.job_id,
                ok,
                requested_pages: state.requested_pages,
                rendered_pages: state.rendered_pages,
                elapsed_ms: saturating_elapsed_ms(state.started_at.elapsed()),
                outputs,
                errors: state.errors,
            };
            self.completed_jobs.fetch_add(1, Ordering::SeqCst);
            Some((result, state.future_listener))
        } else {
            jobs.pending.insert(result.job_id, state);
            None
        }
    }
}

#[derive(Default)]
struct JobBook {
    pending: HashMap<u64, JobState>,
}

struct JobState {
    started_at: Instant,
    requested_pages: usize,
    requested_page_indices: Vec<u32>,
    remaining_shards: usize,
    rendered_pages: usize,
    outputs: Vec<Option<String>>,
    page_to_slot: HashMap<u32, usize>,
    errors: Vec<String>,
    future_listener: Option<PythonFutureListener>,
}

impl JobState {
    fn new(
        prepared: &PreparedJob,
        shard_count: usize,
        future_listener: Option<PythonFutureListener>,
    ) -> Self {
        let page_to_slot = prepared
            .requested_pages
            .iter()
            .enumerate()
            .map(|(slot, page)| (*page, slot))
            .collect::<HashMap<_, _>>();

        Self {
            started_at: Instant::now(),
            requested_pages: prepared.requested_pages.len(),
            requested_page_indices: prepared.requested_pages.clone(),
            remaining_shards: shard_count,
            rendered_pages: 0,
            outputs: vec![None; prepared.requested_pages.len()],
            page_to_slot,
            errors: Vec::new(),
            future_listener,
        }
    }
}

struct PreparedJob {
    spec: JobSpec,
    requested_pages: Vec<u32>,
    token_count: usize,
    pages_per_token: usize,
}

impl PreparedJob {
    fn from_request(job: RenderJob, workers: usize) -> PyResult<Self> {
        let format = OutputFormat::parse(&job.format)?;
        if job.alpha && !format.supports_alpha() {
            return Err(PyValueError::new_err(
                "alpha=True is only supported for png and pam outputs",
            ));
        }

        let output_dir = PathBuf::from(&job.output_dir);
        fs::create_dir_all(&output_dir).map_err(to_py_runtime_error)?;

        let document_pages = inspect_document_page_count(&job.pdf_path, job.password.as_deref())?;
        let requested_pages = resolve_requested_pages(job.page_indices.as_deref(), document_pages)?;
        let (token_count, pages_per_token) =
            scheduling_plan(job.batch_pages, requested_pages.len(), workers);

        let prefix = normalize_prefix(job.prefix, &job.pdf_path);
        let digits = document_pages.max(1).to_string().len().max(4);

        Ok(Self {
            spec: JobSpec {
                pdf_path: job.pdf_path,
                output_dir,
                password: job.password,
                prefix,
                render_dpi: job.render_dpi,
                format,
                quality: job.quality,
                alpha: job.alpha,
                show_extras: job.show_extras,
                digits,
            },
            requested_pages,
            token_count,
            pages_per_token,
        })
    }
}

struct JobSpec {
    pdf_path: String,
    output_dir: PathBuf,
    password: Option<String>,
    prefix: String,
    render_dpi: u32,
    format: OutputFormat,
    quality: u8,
    alpha: bool,
    show_extras: bool,
    digits: usize,
}

enum WorkItem {
    Render(WorkToken),
    Shutdown,
}

struct JobRuntime {
    job_id: u64,
    spec: Arc<JobSpec>,
    pages: Arc<Vec<u32>>,
    next_page: AtomicUsize,
    pages_per_token: usize,
}

struct WorkToken {
    runtime: Arc<JobRuntime>,
}

struct WorkerResult {
    job_id: u64,
    outputs: Vec<PageOutput>,
    errors: Vec<String>,
}

struct PageOutput {
    page_index: u32,
    path: String,
}

#[derive(Clone, Copy)]
enum OutputFormat {
    Png,
    Pnm,
    Pam,
    Psd,
    Ps,
    Jpeg,
}

impl OutputFormat {
    fn parse(value: &str) -> PyResult<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "png" => Ok(Self::Png),
            "pnm" | "ppm" => Ok(Self::Pnm),
            "pam" => Ok(Self::Pam),
            "psd" => Ok(Self::Psd),
            "ps" => Ok(Self::Ps),
            "jpg" | "jpeg" => Ok(Self::Jpeg),
            other => Err(PyValueError::new_err(format!(
                "unsupported format '{other}', expected one of: {}",
                Self::supported().join(", ")
            ))),
        }
    }

    fn supported() -> Vec<&'static str> {
        vec!["pnm", "png", "pam", "psd", "ps", "jpg"]
    }

    fn supports_alpha(self) -> bool {
        matches!(self, Self::Png | Self::Pam)
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Pnm => "pnm",
            Self::Pam => "pam",
            Self::Psd => "psd",
            Self::Ps => "ps",
            Self::Jpeg => "jpg",
        }
    }

    fn as_mupdf(self) -> Option<ImageFormat> {
        Some(match self {
            Self::Png => ImageFormat::PNG,
            Self::Pnm => ImageFormat::PNM,
            Self::Pam => ImageFormat::PAM,
            Self::Psd => ImageFormat::PSD,
            Self::Ps => ImageFormat::PS,
            Self::Jpeg => return None,
        })
    }
}

fn worker_loop(
    _worker_id: usize,
    work_rx: Receiver<WorkItem>,
    result_tx: Sender<WorkerResult>,
    inner: Arc<QueueInner>,
) {
    let mut cached_document = None;
    loop {
        match work_rx.recv() {
            Ok(WorkItem::Render(work)) => {
                inner.queued_shards.fetch_sub(1, Ordering::SeqCst);
                inner.in_progress_shards.fetch_add(1, Ordering::SeqCst);
                let result = render_token(work, &mut cached_document);
                inner.in_progress_shards.fetch_sub(1, Ordering::SeqCst);
                let _ = result_tx.send(result);
            }
            Ok(WorkItem::Shutdown) | Err(_) => break,
        }
    }
}

fn aggregator_loop(
    result_rx: Receiver<WorkerResult>,
    ready_tx: Sender<RenderResult>,
    inner: Arc<QueueInner>,
) {
    while let Ok(result) = result_rx.recv() {
        let Some((result, future_listener)) = inner.ingest_worker_result(result) else {
            continue;
        };

        if let Some(future_listener) = future_listener {
            complete_python_future(future_listener, result);
        } else {
            let _ = ready_tx.send(result);
        }
    }
}

fn render_token(work: WorkToken, cached_document: &mut Option<CachedDocument>) -> WorkerResult {
    let runtime = work.runtime;
    let mut outputs = Vec::new();
    let mut errors = Vec::new();

    let document = match get_or_open_document(cached_document, &runtime.spec) {
        Ok(document) => document,
        Err(err) => {
            errors.push(err);
            return WorkerResult {
                job_id: runtime.job_id,
                outputs,
                errors,
            };
        }
    };

    for _ in 0..runtime.pages_per_token {
        let slot = runtime.next_page.fetch_add(1, Ordering::SeqCst);
        if slot >= runtime.pages.len() {
            break;
        }
        let page_index = runtime.pages[slot];
        match render_single_page(document, &runtime.spec, page_index) {
            Ok(path) => outputs.push(PageOutput { page_index, path }),
            Err(err) => errors.push(format!("page {}: {err}", page_index + 1)),
        }
    }

    WorkerResult {
        job_id: runtime.job_id,
        outputs,
        errors,
    }
}

struct CachedDocument {
    pdf_path: String,
    password: Option<String>,
    document: Document,
}

fn render_single_page(
    document: &Document,
    spec: &JobSpec,
    page_index: u32,
) -> Result<String, String> {
    let page = document
        .load_page(page_index as i32)
        .map_err(|err| format!("failed to load page: {err}"))?;

    let scale = spec.render_dpi as f32 / 72.0;
    let matrix: Matrix = fz_matrix {
        a: scale,
        b: 0.0,
        c: 0.0,
        d: scale,
        e: 0.0,
        f: 0.0,
    }
    .into();

    let colorspace = Colorspace::device_rgb();
    let mut pixmap = page
        .to_pixmap(&matrix, &colorspace, spec.alpha, spec.show_extras)
        .map_err(|err| format!("failed to rasterize page: {err}"))?;
    pixmap.set_resolution(spec.render_dpi as i32, spec.render_dpi as i32);

    let filename = format!(
        "{}_p{:0width$}.{}",
        spec.prefix,
        page_index + 1,
        spec.format.extension(),
        width = spec.digits
    );
    let output_path = spec.output_dir.join(filename);
    let output_str = output_path.to_string_lossy();
    match spec.format.as_mupdf() {
        Some(format) => {
            pixmap
                .save_as(output_str.as_ref(), format)
                .map_err(|err| format!("failed to save image: {err}"))?;
        }
        None => {
            save_pixmap_as_jpeg(&pixmap, &output_path, spec.quality)
                .map_err(|err| format!("failed to save jpeg: {err}"))?;
        }
    }

    Ok(output_path.to_string_lossy().into_owned())
}

fn save_pixmap_as_jpeg(
    pixmap: &mupdf::Pixmap,
    output_path: &Path,
    quality: u8,
) -> Result<(), String> {
    let width = u16::try_from(pixmap.width())
        .map_err(|_| format!("image width {} exceeds jpeg limit", pixmap.width()))?;
    let height = u16::try_from(pixmap.height())
        .map_err(|_| format!("image height {} exceeds jpeg limit", pixmap.height()))?;

    let color_type = match pixmap.n() {
        1 => JpegColorType::Luma,
        3 => JpegColorType::Rgb,
        4 => JpegColorType::Rgba,
        n => return Err(format!("unsupported pixmap component count {n} for jpeg")),
    };

    let writer = BufWriter::new(File::create(output_path).map_err(|err| err.to_string())?);
    let encoder = JpegEncoder::new(writer, quality);
    encoder
        .encode(pixmap.samples(), width, height, color_type)
        .map_err(|err| err.to_string())
}

fn get_or_open_document<'a>(
    cache: &'a mut Option<CachedDocument>,
    spec: &JobSpec,
) -> Result<&'a Document, String> {
    let matches = cache
        .as_ref()
        .is_some_and(|cached| cached.pdf_path == spec.pdf_path && cached.password == spec.password);

    if !matches {
        let document = open_document(&spec.pdf_path, spec.password.as_deref())?;
        *cache = Some(CachedDocument {
            pdf_path: spec.pdf_path.clone(),
            password: spec.password.clone(),
            document,
        });
    }

    Ok(&cache.as_ref().expect("cached document must exist").document)
}

fn create_concurrent_future(py: Python<'_>) -> PyResult<Py<PyAny>> {
    let concurrent = py.import("concurrent.futures")?;
    Ok(concurrent.getattr("Future")?.call0()?.unbind())
}

fn complete_python_future(listener: PythonFutureListener, result: RenderResult) {
    Python::attach(|py| {
        let future = listener.future.bind(py);
        let result = match Py::new(py, result) {
            Ok(result) => result,
            Err(_) => return,
        };
        let _ = future.call_method1("set_result", (result,));
    });
}

fn open_document(path: &str, password: Option<&str>) -> Result<Document, String> {
    let mut document = Document::open(path).map_err(|err| format!("failed to open PDF: {err}"))?;
    let needs_password = document
        .needs_password()
        .map_err(|err| format!("failed to inspect password state: {err}"))?;

    if needs_password {
        let Some(password) = password else {
            return Err("PDF requires a password".to_string());
        };
        let ok = document
            .authenticate(password)
            .map_err(|err| format!("failed to authenticate PDF password: {err}"))?;
        if !ok {
            return Err("PDF password was rejected".to_string());
        }
    }

    Ok(document)
}

fn inspect_document_page_count(path: &str, password: Option<&str>) -> PyResult<usize> {
    let document = open_document(path, password).map_err(PyRuntimeError::new_err)?;
    let page_count = document.page_count().map_err(to_py_runtime_error)?.max(0) as usize;
    if page_count == 0 {
        return Err(PyRuntimeError::new_err("PDF has no pages"));
    }
    Ok(page_count)
}

fn resolve_requested_pages(requested: Option<&[u32]>, page_count: usize) -> PyResult<Vec<u32>> {
    match requested {
        None => Ok((0..page_count as u32).collect()),
        Some(indices) => {
            if indices.is_empty() {
                return Err(PyValueError::new_err("page_indices cannot be empty"));
            }

            let mut pages = indices.to_vec();
            pages.sort_unstable();
            pages.dedup();

            if pages.len() != indices.len() {
                return Err(PyValueError::new_err(
                    "page_indices cannot contain duplicates",
                ));
            }

            let max_index = page_count.saturating_sub(1) as u32;
            if let Some(invalid) = pages.iter().copied().find(|page| *page > max_index) {
                return Err(PyValueError::new_err(format!(
                    "page index {invalid} exceeds document page count {page_count}"
                )));
            }

            Ok(pages)
        }
    }
}

fn normalize_prefix(prefix: Option<String>, pdf_path: &str) -> String {
    let prefix = prefix
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            Path::new(pdf_path)
                .file_stem()
                .and_then(|value| value.to_str())
                .filter(|value| !value.is_empty())
                .unwrap_or("render")
                .to_string()
        });

    prefix
}

fn scheduling_plan(batch_pages: usize, page_count: usize, workers: usize) -> (usize, usize) {
    if page_count == 0 {
        return (0, 0);
    }

    if batch_pages > 0 {
        let pages_per_token = batch_pages.max(1);
        let token_count = page_count.div_ceil(pages_per_token);
        return (token_count, pages_per_token);
    }

    let worker_count = workers.max(1);
    (page_count.min(worker_count), usize::MAX)
}

fn default_worker_count(workers: Option<usize>) -> usize {
    match workers {
        Some(value) if value > 0 => value,
        _ => num_cpus::get_physical().max(1),
    }
}

fn saturating_elapsed_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn to_py_runtime_error(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
