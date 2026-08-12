use crate::*;

pub use decoder::SnapshotBeginDecoder;
pub use encoder::SnapshotBeginEncoder;

pub use crate::SBE_SCHEMA_ID;
pub use crate::SBE_SCHEMA_VERSION;
pub use crate::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 84;
pub const SBE_TEMPLATE_ID: u16 = 52;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct SnapshotBeginEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for SnapshotBeginEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for SnapshotBeginEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> SnapshotBeginEncoder<'a> {
        pub fn wrap(mut self, buf: WriteBuf<'a>, offset: usize) -> Self {
            let limit = offset + SBE_BLOCK_LENGTH as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self
        }

        #[inline]
        pub const fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        #[inline]
        pub fn header(self, offset: usize) -> MessageHeaderEncoder<Self> {
            let mut header = MessageHeaderEncoder::default().wrap(self, offset);
            header.block_length(SBE_BLOCK_LENGTH);
            header.template_id(SBE_TEMPLATE_ID);
            header.schema_id(SBE_SCHEMA_ID);
            header.version(SBE_SCHEMA_VERSION);
            header
        }

        /// primitive field 'engineSeq'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn engine_seq(&mut self, value: u64) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'requestId'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 8
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn request_id(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 8;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'recvTs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn recv_ts(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'bidPrice'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 20
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn bid_price(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 20;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'bidQty'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 28
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn bid_qty(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 28;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'askPrice'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 36
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn ask_price(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 36;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'askQty'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 44
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn ask_qty(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 44;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'restingOrderCount'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 52
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn resting_order_count(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 52;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'lastMdSeq'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 60
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn last_md_seq(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 60;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nextTradeId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 68
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn next_trade_id(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 68;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'riskReferencePrice'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 76
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn risk_reference_price(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 76;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct SnapshotBeginDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for SnapshotBeginDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for SnapshotBeginDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for SnapshotBeginDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> SnapshotBeginDecoder<'a> {
        pub fn wrap(
            mut self,
            buf: ReadBuf<'a>,
            offset: usize,
            acting_block_length: u16,
            acting_version: u16,
        ) -> Self {
            let limit = offset + acting_block_length as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self.acting_block_length = acting_block_length;
            self.acting_version = acting_version;
            self
        }

        #[inline]
        pub const fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        #[inline]
        pub fn header(self, mut header: MessageHeaderDecoder<ReadBuf<'a>>, offset: usize) -> Self {
            debug_assert_eq!(SBE_TEMPLATE_ID, header.template_id());
            let acting_block_length = header.block_length();
            let acting_version = header.version();

            self.wrap(
                header.parent().unwrap(),
                offset + message_header_codec::ENCODED_LENGTH,
                acting_block_length,
                acting_version,
            )
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn engine_seq(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn request_id(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 8)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn recv_ts(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 12)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn bid_price(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 20)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn bid_qty(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 28)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn ask_price(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 36)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn ask_qty(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 44)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn resting_order_count(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 52)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn last_md_seq(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 60)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn next_trade_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 68)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn risk_reference_price(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 76)
        }
    }
} // end decoder
