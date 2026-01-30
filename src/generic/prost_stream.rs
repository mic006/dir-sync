//! Stream Prost messages

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Base size for IO buffers
const PAGE_SIZE: usize = 4096;

/// Read Prost messages from stream
pub struct ProstRead<T> {
    /// Underlying stream
    stream: T,
    /// Buffer for incoming data
    buf: Vec<u8>,
    /// Current read index in `buf`
    read_idx: usize,
}
impl<T> ProstRead<T>
where
    T: AsyncReadExt + Unpin,
{
    /// Wrap stream for reading Prost messages
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(PAGE_SIZE),
            read_idx: 0,
        }
    }

    /// Receive a Prost message from the stream
    ///
    /// /!\ This function may return a message without performing an async call.
    ///
    /// Buffer & read strategy:
    /// - minimize `read()` calls by reading available data from stream and then processing them;
    ///   subsequent calls to `recv()` may return a message directly from the buffer without reading from stream
    /// - if a new `read()` is needed, ensure buffer has enough capacity to hold the expected data (if known)
    /// - if not enough capacity, either move existing data to start of buffer (if fitting and read index near the end of the buffer)
    ///   or expand the buffer (rounded to `PAGE_SIZE`)
    ///
    /// # Errors
    /// - IO error
    /// - Deserialization error
    pub async fn recv<M>(&mut self) -> anyhow::Result<M>
    where
        M: Message + Default,
    {
        // missing data in buffer
        #[allow(clippy::bool_to_int_with_if)]
        let mut data_need = if self.buf.is_empty() {
            // no data: need at least 1 byte (corner case = empty message with 0 as length delimiter)
            1
        } else {
            // use available data
            0
        };

        loop {
            if data_need > 0 {
                // need to read some data: define buffer management strategy
                if data_need > self.buf.capacity() - self.buf.len() {
                    // data does not fit in remaining capacity
                    let used = self.buf.len() - self.read_idx;
                    if used + data_need <= self.buf.capacity()
                        && self.read_idx >= self.buf.capacity() * 3 / 4
                    {
                        // read_idx is in last quarter of buffer, and data fits in buffer
                        // => keep existing buffer, move data to buffer start
                        self.buf.copy_within(self.read_idx.., 0);
                        self.buf.truncate(used);
                        self.read_idx = 0;
                    } else {
                        // expand buffer
                        let rounded_capacity =
                            (self.buf.len() + data_need).div_ceil(PAGE_SIZE) * PAGE_SIZE;
                        self.buf.reserve(rounded_capacity - self.buf.len());
                    }
                }

                // read data from stream
                while data_need > 0 {
                    debug_assert!(
                        self.buf.len() + data_need <= self.buf.capacity(),
                        "strategy failure: buffer capacity is insufficient"
                    );
                    let read_data = self.stream.read_buf(&mut self.buf).await?;
                    anyhow::ensure!(read_data > 0, "stream closed");
                    data_need = data_need.saturating_sub(read_data);
                }
            }

            // try to read message from buffer
            match prost::decode_length_delimiter(&self.buf[self.read_idx..]) {
                Ok(len) => {
                    // full message length known
                    let total_len = prost::length_delimiter_len(len) + len;
                    if self.read_idx + total_len <= self.buf.len() {
                        // full message in buffer
                        let msg = M::decode_length_delimited(
                            &self.buf[self.read_idx..self.read_idx + total_len],
                        )?;
                        self.read_idx += total_len;
                        if self.read_idx >= self.buf.len() {
                            // buffer fully consumed, reset
                            self.buf.clear();
                            self.read_idx = 0;
                        }
                        return Ok(msg);
                    }
                    // need more data for full message
                    data_need = total_len - (self.buf.len() - self.read_idx);
                }
                Err(_) => {
                    // need more data for length delimiter
                    // as length delimiter is on multiple bytes, data len is at least 128 bytes
                    data_need = 128;
                }
            }
        }
    }
}

/// Write Prost messages to stream
pub struct ProstWrite<T> {
    /// Underlying stream
    stream: T,
    /// Buffer for serialized message
    buf: Vec<u8>,
}
impl<T> ProstWrite<T>
where
    T: AsyncWriteExt + Unpin,
{
    /// Wrap stream for writing Prost messages
    pub fn new(stream: T) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(PAGE_SIZE),
        }
    }

    /// Send a Prost message to the stream
    ///
    /// # Errors
    /// - Serialization error
    /// - IO error
    pub async fn send<M>(&mut self, msg: &M) -> anyhow::Result<()>
    where
        M: Message,
    {
        self.buf.clear();
        let msg_size = 10 + msg.encoded_len(); // 10 = max size of length delimiter
        self.buf.reserve(msg_size);
        msg.encode_length_delimited(&mut self.buf)?;
        self.stream.write_all(&self.buf).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    // use duplex instead of simplex: simplex would be sufficient but is buggy (https://github.com/tokio-rs/tokio/issues/6914)
    use tokio::io::{AsyncRead, ReadBuf, duplex};

    #[derive(Clone, PartialEq, Message)]
    struct TestMessage {
        #[prost(int32, tag = "1")]
        number: i32,
        #[prost(string, tag = "2")]
        text: String,
    }

    /// Mock stream with control on the bytes made available to reader
    struct MockStream {
        /// list of data chunks to be read
        chunks: Vec<Vec<u8>>,
        /// Index in chunks
        current_chunk: usize,
    }
    impl MockStream {
        fn new(chunks: Vec<Vec<u8>>) -> Self {
            Self {
                chunks,
                current_chunk: 0,
            }
        }
    }

    impl AsyncRead for MockStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            if self.current_chunk < self.chunks.len() {
                let chunk = &self.chunks[self.current_chunk];
                buf.put_slice(chunk);
                self.current_chunk += 1;
            }
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn test_recv_stream_end() {
        let (pipe_read, _) = duplex(1024);
        let mut reader = ProstRead::new(pipe_read);

        let err = reader.recv::<TestMessage>().await.unwrap_err();
        assert_eq!(err.to_string(), "stream closed");
    }

    #[tokio::test]
    async fn test_send_recv() {
        let (pipe_read, pipe_write) = duplex(1024);
        let mut writer = ProstWrite::new(pipe_write);
        let mut reader = ProstRead::new(pipe_read);

        let msg = TestMessage {
            number: 42,
            text: "hello".to_string(),
        };

        writer.send(&msg).await.unwrap();
        drop(writer);

        let received_msg: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg, received_msg);

        let err = reader.recv::<TestMessage>().await.unwrap_err();
        assert_eq!(err.to_string(), "stream closed");
    }

    #[tokio::test]
    async fn test_empty_message() {
        let (pipe_read, pipe_write) = duplex(1024);
        let mut writer = ProstWrite::new(pipe_write);
        let mut reader = ProstRead::new(pipe_read);

        let msg = TestMessage {
            number: 0,
            text: String::new(),
        };

        let mut encoded_buf = Vec::new();
        msg.encode_length_delimited(&mut encoded_buf).unwrap();
        println!("Encoded msg: {encoded_buf:?}");

        writer.send(&msg).await.unwrap();
        drop(writer);

        let received_msg: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg, received_msg);

        let err = reader.recv::<TestMessage>().await.unwrap_err();
        assert_eq!(err.to_string(), "stream closed");
    }

    #[tokio::test]
    async fn test_multiple_messages() {
        let (pipe_read, pipe_write) = duplex(1024);
        let mut writer = ProstWrite::new(pipe_write);
        let mut reader = ProstRead::new(pipe_read);

        let msg1 = TestMessage {
            number: 1,
            text: "one".to_string(),
        };
        let msg2 = TestMessage {
            number: 2,
            text: "two".to_string(),
        };

        writer.send(&msg1).await.unwrap();
        writer.send(&msg2).await.unwrap();
        drop(writer);

        let received_msg1: TestMessage = reader.recv().await.unwrap();
        let received_msg2: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg1, received_msg1);
        assert_eq!(msg2, received_msg2);

        let err = reader.recv::<TestMessage>().await.unwrap_err();
        assert_eq!(err.to_string(), "stream closed");
    }

    #[tokio::test]
    async fn test_large_message() {
        let (pipe_read, pipe_write) = duplex(PAGE_SIZE * 3);
        let mut writer = ProstWrite::new(pipe_write);
        let mut reader = ProstRead::new(pipe_read);

        let large_text = "a".repeat(PAGE_SIZE * 2);
        let msg = TestMessage {
            number: 123,
            text: large_text,
        };

        writer.send(&msg).await.unwrap();
        drop(writer);

        let received_msg: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg, received_msg);
        assert_eq!(reader.buf.capacity(), PAGE_SIZE * 3);
    }

    #[tokio::test]
    async fn test_move_data_to_buffer_start() {
        let (pipe_read, pipe_write) = duplex(PAGE_SIZE * 2);
        let mut writer = ProstWrite::new(pipe_write);
        let mut reader = ProstRead::new(pipe_read);

        let msg1 = TestMessage {
            number: 1,
            text: "a".repeat(PAGE_SIZE * 3 / 4),
        };
        writer.send(&msg1).await.unwrap();
        let msg2 = TestMessage {
            number: 2,
            text: "b".repeat(PAGE_SIZE * 3 / 4),
        };
        writer.send(&msg2).await.unwrap();
        drop(writer);

        let received_msg1: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg1, received_msg1);
        // After one read, the buffer should not be empty
        assert!(!reader.buf.is_empty());
        assert_ne!(reader.read_idx, 0);

        let initial_capacity = reader.buf.capacity();

        let received_msg2: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg2, received_msg2);

        assert_eq!(
            initial_capacity,
            reader.buf.capacity(),
            "buffer capacity should not change"
        );
    }

    #[tokio::test]
    async fn test_split_length_delimiter() {
        // 1. Create a message that will have a multi-byte length delimiter
        let text = "a".repeat(200);
        let msg = TestMessage { number: 1, text };

        // 2. Encode it
        let mut encoded_buf = Vec::new();
        msg.encode_length_delimited(&mut encoded_buf).unwrap();

        // 3. Split the buffer right after the first byte of the delimiter
        let chunks = vec![encoded_buf[0..1].to_vec(), encoded_buf[1..].to_vec()];

        // 4. Create mock stream
        let mock_stream = MockStream::new(chunks);

        // 5. Use ProstRead to receive
        let mut reader = ProstRead::new(mock_stream);
        let received: TestMessage = reader.recv().await.unwrap();

        assert_eq!(msg, received);
    }

    #[tokio::test]
    async fn test_split_message_payload() {
        let msg = TestMessage {
            number: 42,
            text: "a message split in two".to_string(),
        };

        let mut encoded_buf = Vec::new();
        msg.encode_length_delimited(&mut encoded_buf).unwrap();

        let split_point = encoded_buf.len() / 2;
        let chunks = vec![
            encoded_buf[0..split_point].to_vec(),
            encoded_buf[split_point..].to_vec(),
        ];

        let mock_stream = MockStream::new(chunks);

        let mut reader = ProstRead::new(mock_stream);
        let received: TestMessage = reader.recv().await.unwrap();

        assert_eq!(msg, received);
    }

    #[tokio::test]
    async fn test_read_idx_at_end_of_buffer() {
        let (pipe_read, pipe_write) = duplex(1024);
        let mut writer = ProstWrite::new(pipe_write);
        let mut reader = ProstRead::new(pipe_read);

        // Message 1
        let msg1 = TestMessage {
            number: 1,
            text: "first".to_string(),
        };
        writer.send(&msg1).await.unwrap();

        // Message 2
        let msg2 = TestMessage {
            number: 2,
            text: "second".to_string(),
        };
        writer.send(&msg2).await.unwrap();
        drop(writer);

        // Read message 1
        let recv1: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg1, recv1);
        // After one read, the buffer should not be empty
        assert!(!reader.buf.is_empty());
        assert_ne!(reader.read_idx, 0);

        // Read message 2
        let recv2: TestMessage = reader.recv().await.unwrap();
        assert_eq!(msg2, recv2);
        // After reading all available messages, the buffer should be empty
        assert!(reader.buf.is_empty());
        assert_eq!(reader.read_idx, 0);
    }

    #[tokio::test]
    async fn test_invalid_proto_message() {
        let (pipe_read, mut pipe_write) = duplex(1024);
        let mut reader = ProstRead::new(pipe_read);

        // Some invalid data that is not a valid proto message.
        // A valid message would start with a length delimiter.
        // We send a long string of non-delimiter bytes.
        let invalid_data = vec![0xFF; 20];
        pipe_write.write_all(&invalid_data).await.unwrap();
        drop(pipe_write);

        let result: anyhow::Result<TestMessage> = reader.recv().await;
        assert!(result.is_err());
        // The exact error may vary. It could be a decode error or a "stream closed"
        // error if the logic tries to read more data for a delimiter that never comes.
        // The important part is that it doesn't hang and returns an error.
        let err_string = result.unwrap_err().to_string();
        assert!(
            err_string.contains("decode")
                || err_string.contains("Could not decode")
                || err_string.contains("stream closed"),
            "Error message should indicate a decoding problem or a closed stream, but was: {err_string}"
        );
    }
}
