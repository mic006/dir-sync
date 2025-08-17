use std::time::Duration;

use futures::{Stream, task::Poll};
use pin_project_lite::pin_project;
use tokio::time::Sleep;

trait StreamExt: Stream + Sized {
    /// Limit the rate of the input stream, outputting a single value per period
    ///
    /// If the input stream is producing elements faster than the defined period, extra elements are discarded.
    ///
    /// Note: if the last element cannot be produced immediately, it is retained and output at the period expiry.
    /// The last element is NOT lost, so that the state reported by the output stream is always matching
    /// the state of the input stream, with at most one period of delay.
    fn limit_rate(self, period: Duration) -> StreamRate<Self> {
        StreamRate::new(self, period)
    }
}

impl<S: Sized> StreamExt for S where S: Stream {}

pin_project! {
    /// Stream for the [`limit_rate`](StreamExt::limit_rate) method.
    #[must_use = "streams do nothing unless polled"]
    pub struct StreamRate<S>
    where
        S: Stream,
    {
        #[pin]
        st: S,
        period: Duration,
        #[pin]
        sleep_deadline: Option<Sleep>,
        last_retained_value: Option<S::Item>,
    }
}

impl<S> StreamRate<S>
where
    S: Stream,
{
    fn new(st: S, period: Duration) -> Self {
        Self {
            st,
            period,
            sleep_deadline: None,
            last_retained_value: None,
        }
    }
}

impl<S> Stream for StreamRate<S>
where
    S: Stream,
{
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        // drain the input stream, keeping the last value
        let exhausted = loop {
            match this.st.as_mut().poll_next(cx) {
                Poll::Pending => break false,
                Poll::Ready(Some(val)) => *this.last_retained_value = Some(val),
                Poll::Ready(None) => break true,
            }
        };

        if exhausted {
            // input stream is exhausted: output now the final value or None if the final value has already been output
            this.sleep_deadline.as_mut().set(None);
            Poll::Ready(this.last_retained_value.take())
        } else if let Some(sleep_deadline) = this.sleep_deadline.as_mut().as_pin_mut()
            && !sleep_deadline.is_elapsed()
        {
            // deadline is not met yet
            // wait for next input OR deadline
            let _ = sleep_deadline.poll(cx);
            Poll::Pending
        } else {
            // deadline is met: output last value if any
            if let Some(value) = this.last_retained_value.take() {
                this.sleep_deadline
                    .as_mut()
                    .set(Some(tokio::time::sleep(*this.period)));
                Poll::Ready(Some(value))
            } else {
                // unexpected: woken up while there is no input
                // wait for next input
                this.sleep_deadline.as_mut().set(None);
                Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // at any time, the stream outputs 0 or 1 element due to rate limitation
        (0, Some(1))
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

    use futures::{FutureExt, SinkExt, StreamExt as _};

    use super::*;

    const TEST_STEP: Duration = Duration::from_millis(10);

    /// Step for stream generation
    enum StreamStep<T> {
        /// output the value
        Value(T),
        /// make a pause via `tokio::time::sleep`
        Pause,
    }

    /// Helper to generate a stream of `T` values with a time scale
    async fn stream_step_filter_map<T>(step: StreamStep<T>) -> Option<T> {
        match step {
            StreamStep::Value(v) => Some(v),
            StreamStep::Pause => {
                tokio::time::sleep(TEST_STEP).await;
                None
            }
        }
    }

    /// Generate a stream of `T` values with a time scale
    fn stream_gene<T: 'static>(steps: Vec<StreamStep<T>>) -> Pin<Box<dyn Stream<Item = T>>> {
        Box::pin(futures::stream::iter(steps).filter_map(stream_step_filter_map))
    }

    /// Test `StreamExt::limit_rate` method with a time scaled stream
    #[tokio::test]
    async fn stream_limit_rate() {
        let input = stream_gene(vec![
            StreamStep::Value(1),
            StreamStep::Value(2),
            StreamStep::Value(9),
            StreamStep::Pause,
            StreamStep::Value(11),
            StreamStep::Value(12),
            StreamStep::Pause,
            StreamStep::Value(19),
            StreamStep::Pause, // output 19 at end of period
            StreamStep::Pause,
            StreamStep::Pause,
            StreamStep::Pause,
            StreamStep::Value(29),
            StreamStep::Pause,
            StreamStep::Value(39),
        ]);
        let expected_output = vec![9, 19, 29, 39];
        let output = input.limit_rate(3 * TEST_STEP).collect::<Vec<u32>>().await;
        assert_eq!(output, expected_output);
    }

    /// Test `StreamExt::limit_rate` is properly waking up at the end of a period to output the last known value
    #[tokio::test]
    async fn stream_limit_rate_wakeup_end_of_period() -> anyhow::Result<()> {
        let (mut sender, receiver) = futures::channel::mpsc::unbounded();
        sender.send(1).await?;
        let mut st = Box::pin(receiver.limit_rate(2 * TEST_STEP));
        assert_eq!(st.next().await, Some(1));
        sender.send(2).await?;
        // value is not available right away
        assert_eq!(st.next().now_or_never(), None);
        // and not after half a period
        tokio::time::sleep(TEST_STEP).await;
        assert_eq!(st.next().now_or_never(), None);
        // but value shall be available after one period
        assert_eq!(st.next().await, Some(2));

        Ok(())
    }

    #[test]
    fn stream_limit_rate_size_hint() {
        let st = futures::stream::iter(vec![0]).limit_rate(TEST_STEP);
        assert_eq!(st.size_hint(), (0, Some(1)));
    }
}
