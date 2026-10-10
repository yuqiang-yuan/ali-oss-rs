//! 上传/下载的进度反馈回调。
//!
//! 注意区分：[`crate::object_common::Callback`] 是 OSS 的**服务端**上传回调（回调业务服务器），
//! 而本模块的 [`ProgressCallback`] 是**客户端本地**的进度通知。
//!
//! # 回调契约
//!
//! ## 上传（[`crate::object_common::PutObjectOptions::progress`]）
//!
//! - 首个事件恒为 `(0, Some(total))`，`total` 取自请求的 `content-length`。
//! - 文件 body：每读取约 64 KiB 触发一次，最后一个事件为 `(total, Some(total))`。
//! - 内存 body（`put_object_from_buffer` / `put_object_from_base64`）：数据已在内存中，
//!   只触发 `(0, Some(total))` 和 `(total, Some(total))` 两个事件。
//! - 请求体为 [`crate::request::RequestBody::Empty`] 时（例如 `initiate_multipart_uploads`）不触发任何事件。
//!
//! ## 下载（[`crate::object_common::GetObjectOptions`]）
//!
//! - 首个事件恒为 `(0, total)`，在响应头解析完成后、body 开始读取前触发。
//!   `total` 为 `Some` 就表示长度探测成功（服务端返回了 `Content-Length`）；
//!   为 `None` 表示长度未知（分块传输，或者 `accept_encoding` 生效导致 OSS 省略了该响应头）。
//!   range 下载时 `total` 是本次传输的区间长度，而不是整个 Object 的长度。
//! - 之后每收到一个 chunk 触发一次，最后一个事件为 `(total, Some(total))`。
//!
//! # 线程约定
//!
//! 异步版本的回调在 tokio runtime 的 worker 线程上执行，同步（`blocking`）版本的
//! 上传回调在 reqwest 的工作线程上执行。回调应当**快速返回**：不要阻塞、不要做重 I/O。
//! 回调中 panic 会穿过流的 `poll_next` / `read` 向上展开。

use std::{fmt, sync::Arc};

use bytes::Bytes;
use futures::Stream;

/// 进度回调函数类型：参数为 `(已传输字节数, 总字节数)`。
///
/// 总字节数未知时为 `None`。下载时首个事件的 `total` 是否为 `Some`，
/// 就表示服务端是否返回了 `Content-Length`。
pub type ProgressFn = Arc<dyn Fn(u64, Option<u64>) + Send + Sync>;

/// 上报进度时的读取块大小。
///
/// 太小会导致回调过于频繁（`FramedRead` 默认只有 8 KiB，5 GB 文件会产生 65 万次回调），
/// 太大则会让进度条跳动。
pub(crate) const CHUNK_SIZE: usize = 64 * 1024;

/// 上传/下载进度回调的持有者。
///
/// 默认（[`Default`]）为空，此时不会产生任何回调开销。
#[derive(Clone, Default)]
pub struct ProgressCallback(Option<ProgressFn>);

impl ProgressCallback {
    /// 用闭包创建进度回调。
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(u64, Option<u64>) + Send + Sync + 'static,
    {
        Self(Some(Arc::new(f)))
    }

    /// 是否设置了回调。
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// 触发回调。未设置回调时什么也不做。
    pub fn call(&self, transferred: u64, total: Option<u64>) {
        if let Some(f) = &self.0 {
            f(transferred, total);
        }
    }

    /// 取出内部回调的共享引用，供 crate 内部把回调传递到真正发请求的地方。
    pub(crate) fn as_fn(&self) -> Option<ProgressFn> {
        self.0.clone()
    }
}

impl fmt::Debug for ProgressCallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_some() { "ProgressCallback(set)" } else { "ProgressCallback(none)" })
    }
}

/// 包装一个字节流，在每次成功产出 chunk 后触发进度回调。
pub(crate) struct ProgressStream<S> {
    inner: S,
    callback: ProgressFn,
    transferred: u64,
    total: Option<u64>,
}

impl<S> ProgressStream<S> {
    pub(crate) fn new(inner: S, callback: ProgressFn, total: Option<u64>) -> Self {
        Self {
            inner,
            callback,
            transferred: 0,
            total,
        }
    }
}

impl<S, E> Stream for ProgressStream<S>
where
    S: Stream<Item = std::result::Result<Bytes, E>> + Unpin,
{
    type Item = std::result::Result<Bytes, E>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match std::pin::Pin::new(&mut this.inner).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                this.transferred += chunk.len() as u64;
                (this.callback)(this.transferred, this.total);
                std::task::Poll::Ready(Some(Ok(chunk)))
            }
            other => other,
        }
    }
}

/// 包装一个 [`std::io::Read`]，在每次成功读取后触发进度回调。
#[cfg(feature = "blocking")]
pub(crate) struct ProgressReader<R> {
    inner: R,
    callback: ProgressFn,
    transferred: u64,
    total: Option<u64>,
}

#[cfg(feature = "blocking")]
impl<R> ProgressReader<R> {
    pub(crate) fn new(inner: R, callback: ProgressFn, total: Option<u64>) -> Self {
        Self {
            inner,
            callback,
            transferred: 0,
            total,
        }
    }
}

#[cfg(feature = "blocking")]
impl<R: std::io::Read> std::io::Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;

        if n > 0 {
            self.transferred += n as u64;
            (self.callback)(self.transferred, self.total);
        }

        Ok(n)
    }
}

#[cfg(test)]
mod test_progress {
    use std::sync::Mutex;

    use futures::{executor::block_on, StreamExt};

    use super::*;

    /// 收集到的进度事件。
    type Events = Arc<Mutex<Vec<(u64, Option<u64>)>>>;

    /// 用 Arc<Mutex<..>> 收集回调事件，便于断言。
    fn recorder() -> (Events, ProgressCallback) {
        let events = Arc::new(Mutex::new(Vec::new()));
        let cloned = events.clone();

        let callback = ProgressCallback::new(move |transferred, total| {
            cloned.lock().unwrap().push((transferred, total));
        });

        (events, callback)
    }

    #[test]
    fn test_progress_callback_empty_does_nothing() {
        let cb = ProgressCallback::default();
        assert!(cb.is_empty());
        cb.call(1, Some(2)); // 不应该 panic
    }

    #[test]
    fn test_progress_stream_counts_chunks() {
        let (events, callback) = recorder();

        let chunks: Vec<std::result::Result<Bytes, std::io::Error>> =
            vec![Ok(Bytes::from_static(b"hello")), Ok(Bytes::from_static(b" world")), Ok(Bytes::from_static(b"!"))];

        let stream = ProgressStream::new(futures::stream::iter(chunks), callback.as_fn().unwrap(), Some(12));

        let collected: Vec<Bytes> = block_on(stream.map(|r| r.unwrap()).collect::<Vec<Bytes>>());

        assert_eq!(collected.concat(), b"hello world!".to_vec());
        assert_eq!(*events.lock().unwrap(), vec![(5, Some(12)), (11, Some(12)), (12, Some(12))]);
    }

    #[test]
    fn test_progress_stream_stops_on_error() {
        let (events, callback) = recorder();

        let chunks: Vec<std::result::Result<Bytes, std::io::Error>> =
            vec![Ok(Bytes::from_static(b"abc")), Err(std::io::Error::other("boom"))];

        let stream = ProgressStream::new(futures::stream::iter(chunks), callback.as_fn().unwrap(), Some(100));

        let results = block_on(stream.collect::<Vec<_>>());
        assert_eq!(results.len(), 2);
        assert!(results[1].is_err());

        // 出错后不应该再产生进度事件
        assert_eq!(*events.lock().unwrap(), vec![(3, Some(100))]);
    }

    #[test]
    fn test_progress_stream_unknown_total() {
        let (events, callback) = recorder();

        let chunks: Vec<std::result::Result<Bytes, std::io::Error>> = vec![Ok(Bytes::from_static(b"abcd"))];

        let stream = ProgressStream::new(futures::stream::iter(chunks), callback.as_fn().unwrap(), None);
        let _ = block_on(stream.collect::<Vec<_>>());

        assert_eq!(*events.lock().unwrap(), vec![(4, None)]);
    }

    #[cfg(feature = "blocking")]
    #[test]
    fn test_progress_reader_counts_bytes() {
        let (events, callback) = recorder();

        let mut reader = ProgressReader::new(std::io::Cursor::new(b"0123456789".to_vec()), callback.as_fn().unwrap(), Some(10));

        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut buf).unwrap();

        assert_eq!(buf, b"0123456789".to_vec());

        let events = events.lock().unwrap();
        assert_eq!(events.last(), Some(&(10, Some(10))));
        // 字节数单调递增
        assert!(events.windows(2).all(|w| w[0].0 < w[1].0));
    }
}
