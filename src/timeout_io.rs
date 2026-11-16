use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, Sleep},
};

pub struct IdleTimeoutIo<S> {
    inner: S,
    timeout: Duration,
    timer: Pin<Box<Sleep>>,
}
impl<S> IdleTimeoutIo<S> {
    pub fn new(inner: S, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            timer: Box::pin(tokio::time::sleep(timeout)),
        }
    }
    fn reset(&mut self) {
        self.timer.as_mut().reset(Instant::now() + self.timeout);
    }
    fn poll_idle(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.timer.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection idle timeout",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for IdleTimeoutIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Poll::Ready(result) = self.poll_idle(cx) {
            return Poll::Ready(result);
        }
        let before = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            self.reset();
        }
        result
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for IdleTimeoutIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Poll::Ready(result) = self.poll_idle(cx) {
            return Poll::Ready(result.map(|_| 0));
        }
        let result = Pin::new(&mut self.inner).poll_write(cx, buf);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            self.reset();
        }
        result
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn idle_read_returns_timed_out() {
        let (read_side, _write_side) = tokio::io::duplex(8);
        let mut stream = IdleTimeoutIo::new(read_side, Duration::from_millis(10));
        let mut byte = [0u8; 1];
        let error = stream.read_exact(&mut byte).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
