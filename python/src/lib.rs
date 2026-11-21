//! Python bindings for AWL1 (SPEC.md §7): `agentwire.Writer` appends records to a new file and
//! `agentwire.Reader` reads them back, each body checked by SHA-256. The GIL is released while a
//! record is encoded or rebuilt.

use std::borrow::Cow;
use std::fs::File;
use std::io::{self, BufWriter};
use std::path::PathBuf;

use awl::log;
use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// A file that is not AWL1, or a record that does not rebuild the body it names: `ValueError`.
/// Everything else is the `OSError` it is (`FileExistsError`, `FileNotFoundError`, …).
fn err(e: io::Error) -> PyErr {
    match e.kind() {
        io::ErrorKind::InvalidData => PyValueError::new_err(e.to_string()),
        _ => e.into(),
    }
}

/// Appends records to a new AWL1 file (never an existing one). Each body is stored as a delta
/// against one of the `window` latest records. Use it as a context manager, or `close()` it.
#[pyclass(module = "agentwire")]
struct Writer {
    inner: Option<log::Writer<BufWriter<File>>>,
}

#[pymethods]
impl Writer {
    #[new]
    #[pyo3(signature = (path, window = 16))]
    fn new(path: PathBuf, window: usize) -> PyResult<Self> {
        let file = File::options().write(true).create_new(true).open(path).map_err(err)?;
        let mut w = log::Writer::new(BufWriter::new(file), window).map_err(err)?;
        w.flush().map_err(err)?; // a valid, empty file before the first record
        Ok(Writer { inner: Some(w) })
    }

    /// Appends `body` with its `meta` (stored as is) and returns the record's index.
    #[pyo3(signature = (body, meta = None))]
    fn append(&mut self, py: Python<'_>, body: Cow<'_, [u8]>, meta: Option<Cow<'_, [u8]>>) -> PyResult<u64> {
        let w = self.inner.as_mut().ok_or_else(|| PyValueError::new_err("the writer is closed"))?;
        let (body, meta) = (bytes::Bytes::copy_from_slice(&body), meta.unwrap_or_default().into_owned());
        py.detach(|| w.append(&meta, body)).map_err(err)
    }

    /// Makes the records appended so far readable by others (and safe from a crash of this process).
    fn flush(&mut self) -> PyResult<()> {
        self.inner.as_mut().map_or(Ok(()), |w| w.flush().map_err(err))
    }

    fn close(&mut self) -> PyResult<()> {
        self.flush()?;
        self.inner = None;
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (*_exc))]
    fn __exit__(&mut self, _exc: &Bound<'_, pyo3::types::PyTuple>) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

/// Reads an AWL1 file: `len(r)`, `r[i]` (one record's body), `r.meta(i)`, `r.records()`, and
/// `for meta, body in r` (every record, in order, each rebuilt once). `max_body` caps any body it
/// will rebuild.
#[pyclass(module = "agentwire")]
struct Reader {
    path: PathBuf,
    max_body: usize,
    inner: log::Reader<File>,
}

impl Reader {
    fn index(&self, i: isize) -> PyResult<usize> {
        let n = self.inner.records().len();
        let j = if i < 0 { n.checked_sub(i.unsigned_abs()) } else { Some(i as usize) };
        j.filter(|&j| j < n).ok_or_else(|| PyIndexError::new_err("record index out of range"))
    }
}

#[pymethods]
impl Reader {
    #[new]
    #[pyo3(signature = (path, max_body = 64 << 20))]
    fn new(path: PathBuf, max_body: usize) -> PyResult<Self> {
        let inner = log::Reader::open(File::open(&path).map_err(err)?, max_body).map_err(err)?;
        Ok(Reader { path, max_body, inner })
    }

    fn __len__(&self) -> usize {
        self.inner.records().len()
    }

    /// The file ends inside a record (its writer stopped mid-write): the records before it are whole.
    #[getter]
    fn truncated(&self) -> bool {
        self.inner.truncated
    }

    /// Record `i`'s body, rebuilt from its chain of bases and checked by SHA-256.
    fn get<'py>(&mut self, py: Python<'py>, i: isize) -> PyResult<Bound<'py, PyBytes>> {
        let i = self.index(i)?;
        let inner = &mut self.inner;
        let body = py.detach(|| inner.get(i)).map_err(err)?;
        Ok(PyBytes::new(py, &body))
    }

    fn __getitem__<'py>(&mut self, py: Python<'py>, i: isize) -> PyResult<Bound<'py, PyBytes>> {
        self.get(py, i)
    }

    /// Record `i`'s meta, as it was appended.
    fn meta<'py>(&self, py: Python<'py>, i: isize) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, &self.inner.records()[self.index(i)?].meta))
    }

    /// `(meta, back, stored)` for every record: `back` is how many records back its base is (0: it
    /// is stored whole), `stored` the bytes it takes in the file.
    fn records<'py>(&self, py: Python<'py>) -> Vec<(Bound<'py, PyBytes>, u64, u64)> {
        self.inner.records().iter().map(|r| (PyBytes::new(py, &r.meta), r.back, r.stored)).collect()
    }

    fn __iter__(&self) -> PyResult<Records> {
        let reader = log::Reader::open(File::open(&self.path).map_err(err)?, self.max_body).map_err(err)?;
        Ok(Records { inner: reader.into_iter() })
    }
}

/// `(meta, body)` for every record of a file, in order.
#[pyclass(module = "agentwire")]
struct Records {
    inner: log::Records<File>,
}

#[pymethods]
impl Records {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)>> {
        let inner = &mut self.inner;
        match py.detach(|| inner.next()) {
            None => Ok(None),
            Some(Err(e)) => Err(err(e)),
            Some(Ok((record, body))) => Ok(Some((PyBytes::new(py, &record.meta), PyBytes::new(py, &body)))),
        }
    }
}

#[pymodule]
fn agentwire(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Writer>()?;
    m.add_class::<Reader>()?;
    m.add_class::<Records>()?;
    Ok(())
}
